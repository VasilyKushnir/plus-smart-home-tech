package ru.yandex.practicum.commerce.interactionapi.dto;

import jakarta.validation.constraints.NotNull;
import lombok.Value;

import java.util.Map;
import java.util.UUID;

@Value
public class ProductReturnRequest {
    @NotNull
    UUID orderId;

    @NotNull
    Map<UUID, Integer> products;
}
