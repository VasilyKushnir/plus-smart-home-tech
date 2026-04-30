package ru.yandex.practicum.commerce.payment.service;

import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionapi.dto.PaymentDto;

import java.util.UUID;

public interface PaymentService {
    PaymentDto processPayment(OrderDto order);

    double calculateTotal(OrderDto order);

    void refund(UUID paymentId);

    double calculateProductCost(OrderDto order);

    void denyPayment(UUID paymentId);
}
