package ru.yandex.practicum.commerce.interactionapi.dto;

import jakarta.validation.constraints.NotNull;
import lombok.Value;

import java.util.UUID;

@Value
public class ShippedToDeliveryRequest {
    @NotNull
    UUID orderId;

    @NotNull
    UUID deliveryId;
}
