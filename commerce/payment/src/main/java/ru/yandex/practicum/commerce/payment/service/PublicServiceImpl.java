package ru.yandex.practicum.commerce.payment.service;

import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionapi.dto.PaymentDto;

import java.util.UUID;

public class PublicServiceImpl implements PaymentService {
    @Override
    public PaymentDto processPayment(OrderDto order) {
        return null;
    }

    @Override
    public double calculateTotal(OrderDto order) {
        return 0;
    }

    @Override
    public void refund(UUID paymentId) {

    }

    @Override
    public double calculateProductCost(OrderDto order) {
        return 0;
    }

    @Override
    public void denyPayment(UUID paymentId) {

    }
}
