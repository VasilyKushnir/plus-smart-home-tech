package ru.yandex.practicum.commerce.shoppingcart.service;

import ru.yandex.practicum.commerce.interactionapi.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.interactionapi.dto.ShoppingCartDto;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface ShoppingCartService {
    ShoppingCartDto getShoppingCart(String username);

    ShoppingCartDto addToShoppingCart(String username, Map<UUID, Integer> products);

    void deleteShoppingCart(String username);

    ShoppingCartDto removeFromShoppingCart(String username, Set<UUID> products);

    ShoppingCartDto changeQuantity(String username, ChangeProductQuantityRequest request);
}
