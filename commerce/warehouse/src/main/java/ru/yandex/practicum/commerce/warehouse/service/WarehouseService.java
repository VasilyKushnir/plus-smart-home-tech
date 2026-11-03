package ru.yandex.practicum.commerce.warehouse.service;

import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.commerce.interactionapi.dto.*;

import java.util.Map;
import java.util.UUID;

public interface WarehouseService {
    void putNewProductToWarehouse(NewProductInWarehouseRequest request);

    BookedProductsDto checkProductsInWarehouse(ShoppingCartDto cartDto);

    void addProductToWarehouse(AddProductToWarehouseRequest request);

    AddressDto getWarehouseAddress();

    void shipToDelivery(ShippedToDeliveryRequest request);

    void returnProductsToWarehouse(Map<UUID, Integer> products);

    BookedProductsDto assembly(AssemblyProductsForOrderRequest request);
}
