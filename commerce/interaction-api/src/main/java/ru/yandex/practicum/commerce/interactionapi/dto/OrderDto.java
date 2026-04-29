package ru.yandex.practicum.commerce.interactionapi.dto;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Value;

import java.util.Map;
import java.util.UUID;

@Value
@Builder(toBuilder = true)
public class OrderDto {
    @NotNull
    UUID orderId;

    UUID shoppingCartId;

    @NotNull
    Map<UUID, Integer> products;

    UUID paymentId;

    UUID deliveryId;

    OrderState state;

    double deliveryWeight;

    double deliveryVolume;

    boolean fragile;

    double totalPrice;

    double deliveryPrice;

    double productPrice;
}
