package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val accountExecutors: Map<String, ThreadPoolExecutor> = paymentAccounts
        .associate {
            it.name() to ThreadPoolExecutor(
                it.parallelRequests(),
                it.parallelRequests(),
                1L,
                TimeUnit.SECONDS,
                LinkedBlockingQueue(),
                NamedThreadFactory("payment-submission-executor-${it.name()}")
            )
        }

    private val rateLimiters: Map<String, LeakingBucketRateLimiter> = paymentAccounts
        .associate {
            it.name() to LeakingBucketRateLimiter(
                rate = it.rateLimit().toLong(),
                window = Duration.ofSeconds(1L),
                bucketSize = it.rateLimit()
            )
        }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            val rateLimiter = rateLimiters[account.name()]!!

            accountExecutors[account.name()]?.submit {
                while (!rateLimiter.tick()) {
                    Thread.sleep(5)
                }
                account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
            }
        }
    }
}