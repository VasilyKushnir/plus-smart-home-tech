package ru.yandex.practicum.commerce.warehouse.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.commerce.interactionapi.dto.*;
import ru.yandex.practicum.commerce.interactionapi.feign.WarehouseClient;
import ru.yandex.practicum.commerce.warehouse.entity.OrderBooking;
import ru.yandex.practicum.commerce.warehouse.entity.WarehouseProduct;
import ru.yandex.practicum.commerce.warehouse.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.commerce.warehouse.exception.ProductInWarehouseDoesNotExists;
import ru.yandex.practicum.commerce.warehouse.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.commerce.warehouse.mapper.WarehouseMapper;
import ru.yandex.practicum.commerce.warehouse.repository.OrderBookingRepository;
import ru.yandex.practicum.commerce.warehouse.repository.WarehouseRepository;

import java.security.SecureRandom;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class WarehouseServiceImpl implements WarehouseService {
    private final WarehouseRepository warehouseRepository;
    private final OrderBookingRepository orderBookingRepository;

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
        return this.warehouseContainsProducts(cartDto.getProducts());
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
        OrderBooking orderBooking = orderBookingRepository.findByOrderId(request.getOrderId())
                .orElseThrow(() -> new RuntimeException("Booking order was not found"));
        orderBooking.setDeliveryId(request.getDeliveryId());
        orderBookingRepository.save(orderBooking);
    }

    @Override
    @Transactional
    public void returnProductsToWarehouse(Map<UUID, Integer> products) {
        List<WarehouseProduct> warehouseProducts = warehouseRepository.findAllById(products.keySet());

        if (products.size() != warehouseProducts.size()) {
            throw new RuntimeException("Warehouse does not contain some of required products");
        }

        for (WarehouseProduct warehouseProduct : warehouseProducts) {
            int warehouseProductQuantity = warehouseProduct.getQuantity();
            warehouseProductQuantity += products.get(warehouseProduct.getProductId());
            warehouseProduct.setQuantity(warehouseProductQuantity);
        }
    }


    @Override
    @Transactional
    public BookedProductsDto assembly(AssemblyProductsForOrderRequest request) {
        Map<UUID, Integer> requiredProducts = request.getProducts();
        BookedProductsDto bookedProductsDto = this.warehouseContainsProducts(requiredProducts);

        OrderBooking orderBooking = OrderBooking.builder()
                .orderId(request.getOrderId())
                .products(requiredProducts)
                .build();
        orderBooking = orderBookingRepository.save(orderBooking);

        Collection<WarehouseProduct> warehouseProducts = warehouseRepository.findAllById(orderBooking.getProducts()
                .keySet());

        for (WarehouseProduct warehouseProduct : warehouseProducts) {
            warehouseProduct
                    .setQuantity(warehouseProduct.getQuantity()
                            - requiredProducts.get(warehouseProduct.getProductId()));
        }

        return bookedProductsDto;
    }

    private BookedProductsDto warehouseContainsProducts(Map<UUID, Integer> requiredProducts) {
        Collection<UUID> productIds = requiredProducts.keySet();

        Map<UUID, WarehouseProduct> warehouseProducts = warehouseRepository.findAllById(productIds)
                .stream()
                .collect(Collectors.toMap(WarehouseProduct::getProductId, p -> p));

        if (requiredProducts.size() != warehouseProducts.size()) {
            throw new ProductInWarehouseDoesNotExists("Warehouse does not have some products from the shopping cart");
        }

        double totalWeight = 0.0;
        double totalVolume = 0.0;
        boolean fragile = false;

        for (Map.Entry<UUID, WarehouseProduct> entry : warehouseProducts.entrySet()) {
            UUID productId = entry.getKey();
            WarehouseProduct warehouseProduct = entry.getValue();

            int requestedQuantity = requiredProducts.get(productId);

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
}
