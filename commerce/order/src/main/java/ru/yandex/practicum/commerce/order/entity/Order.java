package ru.yandex.practicum.commerce.order.entity;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderState;

import java.util.Map;
import java.util.UUID;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "orders")
@Getter
@Setter
@Builder(toBuilder = true)
public class Order {
    @Id
    @GeneratedValue
    private UUID orderId;

    private UUID shoppingCartId;

    @ElementCollection
    @CollectionTable(name = "order_products", joinColumns = @JoinColumn(name = "order_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    private Map<UUID, Integer> products;

    private UUID paymentId;

    private UUID deliveryId;

    @Enumerated(EnumType.STRING)
    private OrderState state;

    private double deliveryWeight;

    private double deliveryVolume;

    private boolean fragile;

    private double totalPrice;

    private double deliveryPrice;

    private double productPrice;
}
