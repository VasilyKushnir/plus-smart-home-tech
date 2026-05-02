package ru.yandex.practicum.commerce.order.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.commerce.interactionapi.dto.*;
import ru.yandex.practicum.commerce.interactionapi.feign.DeliveryClient;
import ru.yandex.practicum.commerce.interactionapi.feign.PaymentClient;
import ru.yandex.practicum.commerce.interactionapi.feign.ShoppingCartClient;
import ru.yandex.practicum.commerce.interactionapi.feign.WarehouseClient;
import ru.yandex.practicum.commerce.order.entity.Order;
import ru.yandex.practicum.commerce.order.exception.NoOrderFoundException;
import ru.yandex.practicum.commerce.order.mapper.OrderMapper;
import ru.yandex.practicum.commerce.order.repository.OrderRepository;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {
    private final OrderRepository orderRepository;
    private final ShoppingCartClient shoppingCartClient;
    private final WarehouseClient warehouseClient;
    private final DeliveryClient deliveryClient;
    private final PaymentClient paymentClient;

    @Override
    @Transactional
    public OrderDto createNewOrder(CreateNewOrderRequest request) {
        Order order = Order.builder()
                .state(OrderState.NEW)
                .shoppingCartId(request.getShoppingCart().getShoppingCartId())
                .products(request.getShoppingCart().getProducts())
                .build();

        order = orderRepository.save(order);

        AssemblyProductsForOrderRequest assemblyRequest = AssemblyProductsForOrderRequest.builder()
                .products(order.getProducts())
                .orderId(order.getOrderId())
                .build();

        BookedProductsDto bookedProductsDto = warehouseClient.assembly(assemblyRequest);

        DeliveryDto deliveryDto = DeliveryDto.builder()
                .fromAddress(warehouseClient.getWarehouseAddress())
                .toAddress(request.getDeliveryAddress())
                .orderId(order.getOrderId())
                .build();

        deliveryDto = deliveryClient.addDelivery(deliveryDto);
        order.setDeliveryId(deliveryDto.getDeliveryId());

        order = orderRepository.save(order);

        return OrderMapper.toDto(order);
    }

    @Override
    public Page<OrderDto> getOrders(String username, Pageable pageable) {
        UUID cartId = shoppingCartClient.getShoppingCart(username).getShoppingCartId();
        return orderRepository
                .findAllByShoppingCartId(cartId, pageable)
                .map(OrderMapper::toDto);
    }

    @Override
    public OrderDto returnOrder(ProductReturnRequest request) {
        Order order = this.updateOrderState(request.getOrderId(), OrderState.PRODUCT_RETURNED);
        warehouseClient.returnProductsToWarehouse(request.getProducts());
        return OrderMapper.toDto(order);
    }

    @Override
    public OrderDto payForOrder(UUID orderId) {
        Order order = this.updateOrderState(orderId, OrderState.PAID);
        return OrderMapper.toDto(order);
    }

    @Override
    public OrderDto returnFailedPayment(UUID orderId) {
        Order order = this.updateOrderState(orderId, OrderState.PAYMENT_FAILED);
        return OrderMapper.toDto(order);
    }

    @Override
    public OrderDto deliverOrder(UUID orderId) {
        Order order = this.updateOrderState(orderId, OrderState.DELIVERED);
        return OrderMapper.toDto(order);
    }

    @Override
    public OrderDto returnFailedDelivery(UUID orderId) {
        Order order = this.updateOrderState(orderId, OrderState.DELIVERY_FAILED);
        return OrderMapper.toDto(order);
    }

    @Override
    public OrderDto completeOrder(UUID orderId) {
        Order order = this.updateOrderState(orderId, OrderState.COMPLETED);
        return OrderMapper.toDto(order);
    }

    @Override
    public OrderDto calculateTotal(UUID orderId) {
        Order order = this.getOrder(orderId);
        order.setProductPrice(paymentClient.calculateProductCost(OrderMapper.toDto(order)));
        order.setTotalPrice(paymentClient.calculateTotal(OrderMapper.toDto(order)));

        PaymentDto paymentDto = paymentClient.processPayment(OrderMapper.toDto(order));
        order.setPaymentId(paymentDto.getPaymentId());

        order = orderRepository.save(order);

        return OrderMapper.toDto(order);
    }

    @Override
    public OrderDto calculateDelivery(UUID orderId) {
        Order order = this.getOrder(orderId);
        order.setDeliveryPrice(deliveryClient.returnDeliveryCost(OrderMapper.toDto(order)));
        order = orderRepository.save(order);
        return OrderMapper.toDto(order);
    }

    @Override
    public OrderDto assemblyOrder(UUID orderId) {
        Order order = this.updateOrderState(orderId, OrderState.ASSEMBLED);
        return OrderMapper.toDto(order);
    }

    @Override
    public OrderDto returnFailedAssembly(UUID orderId) {
        Order order = this.updateOrderState(orderId, OrderState.ASSEMBLY_FAILED);
        return OrderMapper.toDto(order);
    }

    private Order getOrder(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order with id: " + orderId + " was not found"));
    }

    private Order updateOrderState(UUID orderId, OrderState state) {
        Order order = this.getOrder(orderId);
        order.setState(state);
        order = orderRepository.save(order);
        return order;
    }
}
