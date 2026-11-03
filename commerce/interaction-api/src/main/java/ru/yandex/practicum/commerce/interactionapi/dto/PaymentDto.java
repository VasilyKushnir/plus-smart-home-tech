package ru.yandex.practicum.commerce.interactionapi.dto;

import lombok.Builder;
import lombok.Value;

import java.util.UUID;

@Value
@Builder(toBuilder = true)
public class PaymentDto {
    UUID paymentId;

    double totalPayment;

    double deliveryTotal;

    double feeTotal;
}
