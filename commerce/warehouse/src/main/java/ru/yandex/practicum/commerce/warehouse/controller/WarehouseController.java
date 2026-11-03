package ru.yandex.practicum.commerce.warehouse.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.interactionapi.dto.*;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;

import java.util.Map;
import java.util.UUID;

@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/warehouse")
public class WarehouseController {
    private final WarehouseService warehouseService;

    @PutMapping
    public void putNewProductToWarehouse(@Valid @RequestBody NewProductInWarehouseRequest request) {
        warehouseService.putNewProductToWarehouse(request);
    }

    @PostMapping("/check")
    public BookedProductsDto checkProductsInWarehouse(@Valid @RequestBody ShoppingCartDto cartDto) {
        return warehouseService.checkProductsInWarehouse(cartDto);
    }

    @PostMapping("/add")
    public void addProductToWarehouse(@Valid @RequestBody AddProductToWarehouseRequest request) {
        warehouseService.addProductToWarehouse(request);
    }

    @GetMapping("/address")
    public AddressDto getWarehouseAddress() {
        return warehouseService.getWarehouseAddress();
    }

    @PostMapping("/shipped")
    public void shipToDelivery(ShippedToDeliveryRequest request) {
        warehouseService.shipToDelivery(request);
    }

    @PostMapping("/return")
    public void returnProductsToWarehouse(@RequestBody @NotEmpty Map<@NotNull UUID, @Positive Integer> products) {
        warehouseService.returnProductsToWarehouse(products);
    }

    @PostMapping("/assembly")
    public BookedProductsDto assembly(@Valid @RequestBody AssemblyProductsForOrderRequest request) {
        return warehouseService.assembly(request);
    }
}
