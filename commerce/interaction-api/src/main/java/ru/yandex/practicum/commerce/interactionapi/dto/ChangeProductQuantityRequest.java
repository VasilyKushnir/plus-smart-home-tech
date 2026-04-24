package ru.yandex.practicum.commerce.interactionapi.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Builder;
import lombok.Value;

import java.util.UUID;

@Value
@Builder
public class ChangeProductQuantityRequest {
    @NotNull
    UUID productId;

    @Positive
    Integer newQuantity;
}
