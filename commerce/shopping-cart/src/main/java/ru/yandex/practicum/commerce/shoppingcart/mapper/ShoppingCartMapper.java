package ru.yandex.practicum.commerce.shoppingcart.mapper;

import lombok.experimental.UtilityClass;
import ru.yandex.practicum.commerce.interactionapi.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.shoppingcart.entity.ShoppingCart;

import java.util.HashMap;

@UtilityClass
public class ShoppingCartMapper {

    public ShoppingCartDto toDto(ShoppingCart shoppingCart) {
        return ShoppingCartDto.builder()
                .shoppingCartId(shoppingCart.getShoppingCartId())
                .products(new HashMap<>(shoppingCart.getProducts()))
                .build();
    }
}
