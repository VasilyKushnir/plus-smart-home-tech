package ru.yandex.practicum.commerce.interactionapi.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Builder;

import java.util.UUID;

@lombok.Value
@Builder(toBuilder = true)
public class AddProductToWarehouseRequest {
    @NotNull
    UUID productId;

    @Positive
    int quantity;
}
