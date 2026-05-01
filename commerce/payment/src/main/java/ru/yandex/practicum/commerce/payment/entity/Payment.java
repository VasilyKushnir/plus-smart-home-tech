package ru.yandex.practicum.commerce.payment.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

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

    private double totalPayment;

    private double deliveryTotal;

    private double feeTotal;
}
