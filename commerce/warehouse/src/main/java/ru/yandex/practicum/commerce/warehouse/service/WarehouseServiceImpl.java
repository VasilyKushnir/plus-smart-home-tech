package ru.yandex.practicum.commerce.warehouse.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.commerce.interactionapi.dto.*;
import ru.yandex.practicum.commerce.warehouse.entity.WarehouseProduct;
import ru.yandex.practicum.commerce.warehouse.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.commerce.warehouse.exception.ProductInWarehouseDoesNotExists;
import ru.yandex.practicum.commerce.warehouse.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.commerce.warehouse.mapper.WarehouseMapper;
import ru.yandex.practicum.commerce.warehouse.repository.WarehouseRepository;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class WarehouseServiceImpl implements WarehouseService {
    private final WarehouseRepository warehouseRepository;

    @Override
    @Transactional
    public void putNewProductToWarehouse(NewProductInWarehouseRequest request) {
        if (warehouseRepository.existsById(request.getProductId())) {
            throw new SpecifiedProductAlreadyInWarehouseException("Product with id " + request.getProductId() +
                    " already exists");
        }
        WarehouseProduct warehouseProduct = WarehouseMapper.toEntity(request);
        warehouseRepository.save(warehouseProduct);
    }

    @Override
    public BookedProductsDto checkProductsInWarehouse(ShoppingCartDto cartDto) {
        Set<UUID> productIds = cartDto.getProducts().keySet();

        Map<UUID, WarehouseProduct> products = warehouseRepository.findAllById(productIds)
                .stream()
                .collect(Collectors.toMap(WarehouseProduct::getProductId, p -> p));

        if (products.size() != productIds.size()) {
            throw new ProductInWarehouseDoesNotExists("Warehouse does not have some products from the shopping cart");
        }

        double totalWeight = 0.0;
        double totalVolume = 0.0;
        boolean fragile = false;

        for (Map.Entry<UUID, WarehouseProduct> entry : products.entrySet()) {
            UUID productId = entry.getKey();
            WarehouseProduct warehouseProduct = entry.getValue();

            int requestedQuantity = cartDto.getProducts().get(productId);

            if (requestedQuantity > warehouseProduct.getQuantity()) {
                throw new ProductInShoppingCartLowQuantityInWarehouse("Warehouse did not have enough products " +
                        "with ID: " + productId + " in stock");
            }

            totalWeight = totalWeight + warehouseProduct.getWeight() * requestedQuantity;
            totalVolume = totalVolume +
                    warehouseProduct.getWidth() * warehouseProduct.getHeight() * warehouseProduct.getDepth()
                            * requestedQuantity;

            if (warehouseProduct.getFragile()) {
                fragile = true;
            }
        }

        return BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(fragile)
                .build();
    }

    @Override
    @Transactional
    public void addProductToWarehouse(AddProductToWarehouseRequest request) {
        WarehouseProduct warehouseProduct = warehouseRepository.findById(request.getProductId())
                .orElseThrow(() -> new ProductInWarehouseDoesNotExists("Product with id " + request.getProductId()
                        + " does not exist"));
        Integer currentQuantity = warehouseProduct.getQuantity();
        if (currentQuantity == null) {
            currentQuantity = 0;
        }
        warehouseProduct.setQuantity(currentQuantity + request.getQuantity());
        warehouseRepository.save(warehouseProduct);
    }

    @Override
    public AddressDto getWarehouseAddress() {
        final String[] ADDRESSES =
                new String[]{"ADDRESS_1", "ADDRESS_2"};

        final String CURRENT_ADDRESS =
                ADDRESSES[Random.from(new SecureRandom()).nextInt(0, ADDRESSES.length)];

        return AddressDto.builder()
                .country(CURRENT_ADDRESS)
                .city(CURRENT_ADDRESS)
                .street(CURRENT_ADDRESS)
                .house(CURRENT_ADDRESS)
                .flat(CURRENT_ADDRESS)
                .build();
    }

    @Override
    public void shipToDelivery(ShippedToDeliveryRequest request) {

    }

    @Override
    public void returnProductsToWarehouse(Map<UUID, Integer> products) {

    }

    @Override
    public BookedProductsDto assembly(AssemblyProductsForOrderRequest request) {
        return null;
    }
}
