package ru.yandex.practicum.commerce.interactionapi.dto;

import jakarta.validation.constraints.Positive;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class DimensionDto {
    @Positive
    double width;

    @Positive
    double height;

    @Positive
    double depth;
}
