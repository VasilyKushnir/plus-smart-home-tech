package ru.yandex.practicum.commerce.shoppingstore.entity;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.interactionapi.dto.QuantityState;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductState;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductCategory;

import java.util.UUID;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "products")
@Getter
@Setter
@Builder(toBuilder = true)
public class Product {
    @Id
    @GeneratedValue
    private UUID productId;

    private String productName;

    private String description;

    private String imageSrc;

    @Enumerated(EnumType.STRING)
    private QuantityState quantityState;

    @Enumerated(EnumType.STRING)
    private ProductState productState;

    @Enumerated(EnumType.STRING)
    private ProductCategory productCategory;

    private Double price;
}
