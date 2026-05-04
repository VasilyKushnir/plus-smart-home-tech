package ru.yandex.practicum.commerce.order.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import ru.yandex.practicum.commerce.interactionapi.dto.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductReturnRequest;

import java.util.UUID;

public interface OrderService {
    OrderDto createNewOrder(CreateNewOrderRequest request);

    Page<OrderDto> getOrders(String username, Pageable pageable);

    OrderDto returnOrder(ProductReturnRequest request);

    OrderDto payForOrder(UUID orderId);

    OrderDto returnFailedPayment(UUID orderId);

    OrderDto deliverOrder(UUID orderId);

    OrderDto returnFailedDelivery(UUID orderId);

    OrderDto completeOrder(UUID orderId);

    OrderDto calculateTotal(UUID orderId);

    OrderDto calculateDelivery(UUID orderId);

    OrderDto assemblyOrder(UUID orderId);

    OrderDto returnFailedAssembly(UUID orderId);
}
