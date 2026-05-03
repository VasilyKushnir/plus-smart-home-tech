package ru.yandex.practicum.commerce.delivery.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.commerce.delivery.entity.Address;
import ru.yandex.practicum.commerce.delivery.entity.Delivery;
import ru.yandex.practicum.commerce.delivery.exception.NoDeliveryFoundException;
import ru.yandex.practicum.commerce.delivery.mapper.DeliveryMapper;
import ru.yandex.practicum.commerce.delivery.repository.DeliveryRepository;
import ru.yandex.practicum.commerce.interactionapi.dto.DeliveryDto;
import ru.yandex.practicum.commerce.interactionapi.dto.DeliveryState;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionapi.dto.ShippedToDeliveryRequest;
import ru.yandex.practicum.commerce.interactionapi.feign.OrderClient;
import ru.yandex.practicum.commerce.interactionapi.feign.WarehouseClient;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DeliveryServiceImpl implements DeliveryService {
    private final DeliveryRepository deliveryRepository;
    private final OrderClient orderClient;
    private final WarehouseClient warehouseClient;

    @Override
    public DeliveryDto addDelivery(DeliveryDto deliveryDto) {
        Delivery delivery = DeliveryMapper.toEntity(deliveryDto);
        delivery.setDeliveryState(DeliveryState.CREATED);
        delivery = deliveryRepository.save(delivery);
        return DeliveryMapper.toDto(delivery);
    }

    @Override
    public void returnSuccessfulDelivery(UUID deliveryId) {
        Delivery delivery = this.getDelivery(deliveryId);
        delivery.setDeliveryState(DeliveryState.DELIVERED);
        deliveryRepository.save(delivery);
        orderClient.deliverOrder(delivery.getOrderId());
    }

    @Override
    public void returnPickedDelivery(UUID deliveryId) {
        Delivery delivery = this.getDelivery(deliveryId);
        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
        orderClient.assemblyOrder(delivery.getOrderId());
        warehouseClient.shipToDelivery(ShippedToDeliveryRequest.builder()
                .orderId(delivery.getOrderId())
                .deliveryId(deliveryId)
                .build());
    }

    @Override
    public void returnFailedDelivery(UUID deliveryId) {
        Delivery delivery = this.getDelivery(deliveryId);
        delivery.setDeliveryState(DeliveryState.FAILED);
        deliveryRepository.save(delivery);
        orderClient.returnFailedDelivery(delivery.getOrderId());
    }

    @Override
    public double returnDeliveryCost(OrderDto orderDto) {
        Delivery delivery = this.getDelivery(orderDto.getDeliveryId());

        Address warehouseAddress = delivery.getFromAddress();
        Address deliveryAddress = delivery.getToAddress();

        double deliveryWeight = orderDto.getDeliveryWeight();
        double deliveryVolume = orderDto.getDeliveryVolume();
        boolean isFragile = orderDto.getFragile();

        double totalCost = 5.0;

        if (warehouseAddress.getStreet().equals("ADDRESS_2")) {
            totalCost += totalCost * 2;
        }

        if (isFragile) {
            totalCost += totalCost * 0.2;
        }

        totalCost += deliveryWeight * 0.3;
        totalCost += deliveryVolume * 0.2;

        if (!warehouseAddress.getStreet().equals(deliveryAddress.getStreet())) {
            totalCost += totalCost * 0.2;
        }

        return totalCost;
    }

    private Delivery getDelivery(UUID deliveryId) {
        return deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new NoDeliveryFoundException("Delivery with id: " + deliveryId + " was not found"));
    }
}
