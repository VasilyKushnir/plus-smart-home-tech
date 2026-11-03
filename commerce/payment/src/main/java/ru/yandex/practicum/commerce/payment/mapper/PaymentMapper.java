package ru.yandex.practicum.commerce.payment.mapper;

import lombok.experimental.UtilityClass;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionapi.dto.PaymentDto;
import ru.yandex.practicum.commerce.interactionapi.dto.PaymentStatus;
import ru.yandex.practicum.commerce.payment.entity.Payment;

@UtilityClass
public class PaymentMapper {

    public Payment fromOrderDtoToPayment(OrderDto order, double feeTotal) {
        return Payment.builder()
                .orderId(order.getOrderId())
                .paymentStatus(PaymentStatus.PENDING)
                .totalPayment(order.getTotalPrice())
                .deliveryTotal(order.getDeliveryPrice())
                .feeTotal(feeTotal)
                .build();
    }

    public PaymentDto toDto(Payment payment) {
        return PaymentDto.builder()
                .paymentId(payment.getPaymentId())
                .totalPayment(payment.getTotalPayment())
                .deliveryTotal(payment.getDeliveryTotal())
                .feeTotal(payment.getFeeTotal())
                .build();
    }
}
