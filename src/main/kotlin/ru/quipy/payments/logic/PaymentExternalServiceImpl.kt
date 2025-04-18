package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import okhttp3.ConnectionPool
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


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

    private val client = OkHttpClient.Builder()
        .readTimeout(Duration.ofMillis((requestAverageProcessingTime.toMillis() * 1.4).toLong()))
        .build()

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val maxRetries = 3
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        var result: ExternalSysResponse? = null
        try {
            for (i in 0 until maxRetries) {
                if (!irrelevantRequest(deadline)) {
                    while (!rateLimiter.tick()) {
                        delay(10)
                    }
                    if (!irrelevantRequest(deadline)) {
                        try {
                            semaphore.acquire()
                            result = client.newCall(request).execute().use { response ->
                                try {
                                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                                } catch (e: Exception) {
                                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                                    ExternalSysResponse(
                                        transactionId.toString(),
                                        paymentId.toString(),
                                        false,
                                        e.message
                                    )
                                }
                            }
                        } finally {
                            semaphore.release()
                        }
                    }
                }


                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${result!!.result}, message: ${result!!.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(result!!.result, now(), transactionId, reason = result!!.message)
                }

                return // Выход из метода, если запрос успешен
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error(
                        "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId ",
                        e
                    )
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error(
                        "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId ",
                        e
                    )
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }

        }
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), null, reason = "Failed")
        }
    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun irrelevantRequest(deadline: Long): Boolean =
        now() + requestAverageProcessingTime.toMillis() * 1.4 >= deadline
}

public fun now() = System.currentTimeMillis()

