package ru.yandex.practicum.commerce.delivery.service;

import ru.yandex.practicum.commerce.interactionapi.dto.DeliveryDto;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;

import java.util.UUID;

public interface DeliveryService {
    DeliveryDto addDelivery(DeliveryDto deliveryDto);

    void returnSuccessfulDelivery(UUID orderId);

    void returnPickedDelivery(UUID orderId);

    void returnFailedDelivery(UUID orderId);

    double returnDeliveryCost(OrderDto orderDto);
}
