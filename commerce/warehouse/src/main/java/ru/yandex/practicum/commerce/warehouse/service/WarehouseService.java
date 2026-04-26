package ru.yandex.practicum.commerce.warehouse.service;

import ru.yandex.practicum.commerce.interactionapi.dto.*;

public interface WarehouseService {
    void putNewProductToWarehouse(NewProductInWarehouseRequest request);

    BookedProductsDto checkProductsInWarehouse(ShoppingCartDto cartDto);

    void addProductToWarehouse(AddProductToWarehouseRequest request);

    AddressDto getWarehouseAddress();
}
