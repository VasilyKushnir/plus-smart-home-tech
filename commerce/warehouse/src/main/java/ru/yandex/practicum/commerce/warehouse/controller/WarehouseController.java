package ru.yandex.practicum.commerce.warehouse.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.interactionapi.dto.*;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;

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

    @GetMapping("/adress")
    public AddressDto getWarehouseAddress() {
        return warehouseService.getWarehouseAddress();
    }
}
