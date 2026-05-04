package ru.yandex.practicum.commerce.delivery.entity;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "addresses")
@Getter
@Setter
@Builder(toBuilder = true)
public class Address {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID addressId;

    private String country;

    private String city;

    private String street;

    private String house;

    private String flat;
}
