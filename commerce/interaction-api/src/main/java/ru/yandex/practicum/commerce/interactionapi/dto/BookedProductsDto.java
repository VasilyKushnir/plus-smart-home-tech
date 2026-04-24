package ru.yandex.practicum.commerce.interactionapi.dto;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class BookedProductsDto {
    double deliveryWeight;
    double deliveryVolume;
    boolean fragile;
}
