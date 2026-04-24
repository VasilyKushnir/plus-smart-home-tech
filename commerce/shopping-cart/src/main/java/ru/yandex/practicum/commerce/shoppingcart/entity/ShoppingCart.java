package ru.yandex.practicum.commerce.shoppingcart.entity;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.interactionapi.dto.ShoppingCartState;

import java.util.Map;
import java.util.UUID;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "shopping_carts")
@Getter
@Setter
@Builder(toBuilder = true)
public class ShoppingCart {
    @Id
    @GeneratedValue
    private UUID shoppingCartId;

    private String username;

    @Enumerated(EnumType.STRING)
    private ShoppingCartState state;

    @ElementCollection
    @CollectionTable(name = "shopping_cart_products", joinColumns = @JoinColumn(name = "cart_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    private Map<UUID, Integer> products;
}
