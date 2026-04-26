package ru.yandex.practicum.commerce.interactionapi.dto;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;

import java.util.Map;
import java.util.UUID;

@lombok.Value
@Builder(toBuilder = true)
public class ShoppingCartDto {
    @NotNull
    UUID shoppingCardId;

    Map<UUID, Integer> products;
}
