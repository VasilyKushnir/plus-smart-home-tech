package ru.yandex.practicum.commerce.warehouse.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.util.UUID;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "warehouse_product")
@Getter
@Setter
@Builder(toBuilder = true)
public class WarehouseProduct {
    @Id
    private UUID productId;

    private Integer quantity;

    private Boolean fragile;

    private double width;

    private double height;

    private double depth;

    private double weight;
}
