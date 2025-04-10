package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.net.http.HttpClient
import java.net.http.HttpClient.Version
import java.time.Duration
import java.util.*
import java.util.concurrent.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimiter = LeakingBucketRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1L),
        bucketSize = properties.rateLimitPerSec
    )
    private val semaphore = Semaphore(properties.parallelRequests)
    private val requestAverageProcessingTime = properties.averageProcessingTime

    private val client = HttpClient.newBuilder()
        .version(Version.HTTP_2)
        .executor(Executors.newFixedThreadPool(50))
        .connectTimeout(Duration.ofMillis(properties.averageProcessingTime.toMillis()))
        .build();

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Void> {
        val maxRetries = 0
        val transactionId = UUID.randomUUID()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val url = "http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

        val future = CompletableFuture<Void>()

        fun executeAttempt(attempt: Int) {
            if (irrelevantRequest(deadline)) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request is irrelevant")
                }
                future.complete(null)
                return
            }

            val request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(url))
                .POST(java.net.http.HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofMillis((properties.averageProcessingTime.toMillis() * 1.4).toLong()))
                .build()

            client.sendAsync(request, java.net.http.HttpResponse.BodyHandlers.ofString())
                .thenAccept { response ->
                    semaphore.release()

                    val body = response.body()
                    val result = try {
                        mapper.readValue(body, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Failed to parse response: $body", e)
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentId.toString(),
                            false,
                            e.message
                        )
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${result.result}, message: ${result.message}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(result.result, now(), transactionId, reason = result.message)
                    }

                    future.complete(null)
                }
                .exceptionally { ex ->
                    val reason = when (ex.cause) {
                        is SocketTimeoutException -> "Request timeout."
                        else -> ex.message ?: "Unknown error"
                    }

                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", ex)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason)
                    }

                    if (attempt + 1 < maxRetries) {
                        executeAttempt(attempt + 1)
                    } else {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), null, reason = "Failed")
                        }
                        future.complete(null)
                    }
                    null
                }
        }

        executeAttempt(0)
        return future
    }




    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun irrelevantRequest(deadline: Long): Boolean =
        now() + requestAverageProcessingTime.toMillis() * 1.4 >= deadline
}

public fun now() = System.currentTimeMillis()

