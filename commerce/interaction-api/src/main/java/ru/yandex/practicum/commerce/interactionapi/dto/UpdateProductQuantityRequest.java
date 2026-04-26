package ru.yandex.practicum.commerce.interactionapi.dto;

import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.UUID;


@Value
public class UpdateProductQuantityRequest {
    @NotNull
    UUID productId;

    @NotNull
    QuantityState quantityState;
}
