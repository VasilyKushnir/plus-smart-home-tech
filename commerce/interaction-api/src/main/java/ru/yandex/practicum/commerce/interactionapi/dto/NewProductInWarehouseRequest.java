package ru.yandex.practicum.commerce.interactionapi.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Value;

import java.util.UUID;

@Value
public class NewProductInWarehouseRequest {
    @NotNull
    UUID productId;

    Boolean fragile;

    @NotNull @Valid
    DimensionDto dimension;

    @Positive
    double weight;
}
