package ru.yandex.practicum.commerce.interactionapi.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.commerce.interactionapi.dto.*;

import java.util.Map;
import java.util.UUID;

@FeignClient(name = "warehouse", path = "/api/v1/warehouse")
public interface WarehouseClient {
    @PutMapping
    void putNewProductToWarehouse(@RequestBody NewProductInWarehouseRequest request);

    @PostMapping("/check")
    BookedProductsDto checkProductsInWarehouse(@RequestBody ShoppingCartDto cartDto);

    @PostMapping("/add")
    void addProductToWarehouse(@RequestBody AddProductToWarehouseRequest request);

    @GetMapping("/adress")
    AddressDto getWarehouseAddress();

    @PostMapping("/shipped")
    void shipToDelivery(ShippedToDeliveryRequest request);

    @PostMapping("/return")
    void returnProductsToWarehouse(@RequestBody Map<UUID, Integer> products);

    @PostMapping("/assembly")
    BookedProductsDto assembly(@RequestBody AssemblyProductsForOrderRequest request);
}
