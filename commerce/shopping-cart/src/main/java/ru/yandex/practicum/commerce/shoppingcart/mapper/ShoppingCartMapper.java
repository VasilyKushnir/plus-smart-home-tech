package ru.yandex.practicum.commerce.shoppingcart.mapper;

import ru.yandex.practicum.commerce.interactionapi.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.shoppingcart.entity.ShoppingCart;

import java.util.HashMap;

public class ShoppingCartMapper {
    public static ShoppingCartDto toDto(ShoppingCart shoppingCart) {
        return ShoppingCartDto.builder()
                .shoppingCardId(shoppingCart.getShoppingCartId())
                .products(new HashMap<>(shoppingCart.getProducts()))
                .build();
    }
}
