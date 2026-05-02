package ru.yandex.practicum.commerce.interactionapi.dto;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Value;

import java.util.Map;
import java.util.UUID;

@Value
@Builder(toBuilder = true)
public class AssemblyProductsForOrderRequest {
    @NotNull
    Map<UUID, Integer> products;

    @NotNull
    UUID orderId;
}
