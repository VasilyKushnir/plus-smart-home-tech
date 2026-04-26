package ru.yandex.practicum.commerce.shoppingcart.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.commerce.interactionapi.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.interactionapi.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.interactionapi.dto.ShoppingCartState;
import ru.yandex.practicum.commerce.interactionapi.feign.WarehouseClient;
import ru.yandex.practicum.commerce.shoppingcart.entity.ShoppingCart;
import ru.yandex.practicum.commerce.shoppingcart.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.commerce.shoppingcart.exception.NotAuthorizedUserException;
import ru.yandex.practicum.commerce.shoppingcart.mapper.ShoppingCartMapper;
import ru.yandex.practicum.commerce.shoppingcart.repository.ShoppingCartRepository;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ShoppingCartServiceImpl implements ShoppingCartService {
    private final ShoppingCartRepository shoppingCartRepository;
    private final WarehouseClient warehouseClient;

    @Override
    public ShoppingCartDto getShoppingCart(String username) {
        ShoppingCart shoppingCart = this.returnShoppingCart(username);
        return ShoppingCartMapper.toDto(shoppingCart);
    }

    @Override
    @Transactional
    public ShoppingCartDto addToShoppingCart(String username, Map<UUID, Integer> products) {
        ShoppingCart currentShoppingCart = this.returnShoppingCart(username);
        currentShoppingCart.getProducts().putAll(products);
        warehouseClient.checkProductsInWarehouse(ShoppingCartMapper.toDto(currentShoppingCart));
        ShoppingCart updatedShoppingCart = shoppingCartRepository.save(currentShoppingCart);
        return ShoppingCartMapper.toDto(updatedShoppingCart);
    }

    @Override
    @Transactional
    public void deleteShoppingCart(String username) {
        ShoppingCart cart = shoppingCartRepository.findByUsernameAndState(username, ShoppingCartState.ACTIVE)
                .orElseThrow(() -> new NotAuthorizedUserException("Shopping cart for user: " + username + " was" +
                        " not found"));
        cart.setState(ShoppingCartState.DEACTIVATE);
        shoppingCartRepository.save(cart);
    }

    @Override
    @Transactional
    public ShoppingCartDto removeFromShoppingCart(String username, Set<UUID> products) {
        ShoppingCart cart = this.returnShoppingCart(username);
        for (UUID productId : products) {
            cart.getProducts().remove(productId);
        }
        return ShoppingCartMapper.toDto(shoppingCartRepository.save(cart));
    }

    @Override
    @Transactional
    public ShoppingCartDto changeQuantity(String username, ChangeProductQuantityRequest request) {
        ShoppingCart cart = this.returnShoppingCart(username);

        if (cart.getProducts().containsKey(request.getProductId())) {
            cart.getProducts().put(request.getProductId(), request.getNewQuantity());
            shoppingCartRepository.save(cart);
            return ShoppingCartMapper.toDto(cart);
        }

        throw new NoProductsInShoppingCartException("Product was not found");
    }

    private ShoppingCart returnShoppingCart(String username) {
        return shoppingCartRepository.findByUsernameAndState(username,
                        ShoppingCartState.ACTIVE)
                .orElseGet(() -> {
                    ShoppingCart newShoppingCart = ShoppingCart.builder()
                            .username(username)
                            .state(ShoppingCartState.ACTIVE)
                            .products(new HashMap<>())
                            .build();
                    return shoppingCartRepository.save(newShoppingCart);
                });
    }
}
