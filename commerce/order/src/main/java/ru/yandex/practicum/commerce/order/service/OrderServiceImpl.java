package ru.yandex.practicum.commerce.order.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.commerce.interactionapi.dto.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductReturnRequest;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    @Override
    public OrderDto createNewOrder(CreateNewOrderRequest request) {
        return null;
    }

    @Override
    public Page<OrderDto> getOrders(String username, Pageable pageable) {
        return null;
    }

    @Override
    public OrderDto returnOrder(ProductReturnRequest request) {
        return null;
    }

    @Override
    public OrderDto payForOrder(UUID orderId) {
        return null;
    }

    @Override
    public OrderDto returnFailedPayment(UUID orderId) {
        return null;
    }

    @Override
    public OrderDto deliverOrder(UUID orderId) {
        return null;
    }

    @Override
    public OrderDto returnFailedDelivery(UUID orderId) {
        return null;
    }

    @Override
    public OrderDto completeOrder(UUID orderId) {
        return null;
    }

    @Override
    public OrderDto calculateTotal(UUID orderId) {
        return null;
    }

    @Override
    public OrderDto calculateDelivery(UUID orderId) {
        return null;
    }

    @Override
    public OrderDto assemblyOrder(UUID orderId) {
        return null;
    }

    @Override
    public OrderDto returnFailedAssembly(UUID orderId) {
        return null;
    }
}
