package ru.yandex.practicum.commerce.payment.entity;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.interactionapi.dto.PaymentStatus;

import java.util.UUID;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "payments")
@Getter
@Setter
@Builder(toBuilder = true)
public class Payment {
    @Id
    @GeneratedValue
    private UUID paymentId;

    private UUID orderId;

    @Enumerated(EnumType.STRING)
    PaymentStatus paymentStatus;

    private double totalPayment;

    private double deliveryTotal;

    private double feeTotal;
}
