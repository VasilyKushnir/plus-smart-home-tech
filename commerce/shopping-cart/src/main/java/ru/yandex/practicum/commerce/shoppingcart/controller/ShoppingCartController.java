package ru.yandex.practicum.commerce.shoppingcart.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.interactionapi.dto.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.interactionapi.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.shoppingcart.service.ShoppingCartService;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-cart")
public class ShoppingCartController {
    private final ShoppingCartService shoppingCartService;

    @GetMapping
    public ShoppingCartDto getShoppingCart(@RequestParam @NotBlank String username) {
        return shoppingCartService.getShoppingCart(username);
    }

    @PutMapping
    public ShoppingCartDto addToShoppingCart(@RequestParam @NotBlank String username,
                                             @Valid @RequestBody Map<@NotNull UUID, @Positive Integer> products) {
        return shoppingCartService.addToShoppingCart(username, products);
    }

    @DeleteMapping
    public void deleteShoppingCart(@RequestParam @NotBlank String username) {
        shoppingCartService.deleteShoppingCart(username);
    }

    @PostMapping("/remove")
    public ShoppingCartDto removeFromShoppingCart(@RequestParam @NotBlank String username,
                                                  @Valid @RequestBody @NotEmpty Set<@NotNull UUID> products) {
        return shoppingCartService.removeFromShoppingCart(username, products);
    }

    @PostMapping("/change-quantity")
    public ShoppingCartDto changeQuantity(@RequestParam @NotBlank String username,
                                          @Valid @RequestBody ChangeProductQuantityRequest request) {
        return shoppingCartService.changeQuantity(username, request);
    }
}
