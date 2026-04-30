package ru.yandex.practicum.commerce.delivery.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.commerce.interactionapi.dto.DeliveryDto;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DeliveryServiceImpl implements DeliveryService {
    @Override
    public DeliveryDto addDelivery(DeliveryDto deliveryDto) {
        return null;
    }

    @Override
    public void returnSuccessfulDelivery(UUID orderId) {

    }

    @Override
    public void returnPickedDelivery(UUID orderId) {

    }

    @Override
    public void returnFailedDelivery(UUID orderId) {

    }

    @Override
    public double returnDeliveryCost(OrderDto orderDto) {
        return 0;
    }
}
