package ru.yandex.practicum.commerce.warehouse.exception;

public class ProductInWarehouseDoesNotExists extends RuntimeException {
    public ProductInWarehouseDoesNotExists(String message) {
        super(message);
    }
}
