package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*

@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService
) {

    val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val createdEvent = paymentESService.create {
            it.create(
                paymentId,
                orderId,
                amount
            )
        }
        logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

        paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        return createdAt
    }
}