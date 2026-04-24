package ru.yandex.practicum.commerce.warehouse.mapper;

import ru.yandex.practicum.commerce.interactionapi.dto.NewProductInWarehouseRequest;
import ru.yandex.practicum.commerce.warehouse.entity.WarehouseProduct;

public class WarehouseMapper {
    public static WarehouseProduct toEntity(NewProductInWarehouseRequest request) {
        return WarehouseProduct.builder()
                .productId(request.getProductId())
                .fragile(request.getFragile())

                .width(request.getDimension().getWidth())
                .height(request.getDimension().getHeight())
                .depth(request.getDimension().getDepth())

                .weight(request.getWeight())
                .build();
    }
}
