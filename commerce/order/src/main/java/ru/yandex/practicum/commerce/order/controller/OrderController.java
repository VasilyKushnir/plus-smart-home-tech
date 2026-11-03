package ru.yandex.practicum.commerce.order.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.interactionapi.dto.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductReturnRequest;
import ru.yandex.practicum.commerce.order.service.OrderService;

import java.util.UUID;

@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/order")
public class OrderController {
    private final OrderService orderService;

    @PutMapping
    public OrderDto createNewOrder(@Valid @RequestBody CreateNewOrderRequest request) {
        return orderService.createNewOrder(request);
    }

    @GetMapping
    public Page<OrderDto> getOrders(
            @RequestParam @NotBlank String username,
            @PageableDefault(size = 20) Pageable pageable) {
        return orderService.getOrders(username, pageable);
    }

    @PostMapping("/return")
    public OrderDto returnOrder(@Valid @RequestBody ProductReturnRequest request) {
        return orderService.returnOrder(request);
    }

    @PostMapping("/payment")
    public OrderDto payForOrder(@RequestBody @NotBlank UUID orderId) {
        return orderService.payForOrder(orderId);
    }

    @PostMapping("/payment/failed")
    public OrderDto returnFailedPayment(@RequestBody @NotBlank UUID orderId) {
        return orderService.returnFailedPayment(orderId);
    }

    @PostMapping("/delivery")
    public OrderDto deliverOrder(@RequestBody @NotBlank UUID orderId) {
        return orderService.deliverOrder(orderId);
    }

    @PostMapping("/delivery/failed")
    public OrderDto returnFailedDelivery(@RequestBody @NotBlank UUID orderId) {
        return orderService.returnFailedDelivery(orderId);
    }

    @PostMapping("/completed")
    public OrderDto completeOrder(@RequestBody @NotBlank UUID orderId) {
        return orderService.completeOrder(orderId);
    }

    @PostMapping("/calculate/total")
    public OrderDto calculateTotal(@RequestBody @NotBlank UUID orderId) {
        return orderService.calculateTotal(orderId);
    }

    @PostMapping("/calculate/delivery")
    public OrderDto calculateDelivery(@RequestBody @NotBlank UUID orderId) {
        return orderService.calculateDelivery(orderId);
    }

    @PostMapping("/assembly")
    public OrderDto assemblyOrder(@RequestBody @NotBlank UUID orderId) {
        return orderService.assemblyOrder(orderId);
    }

    @PostMapping("/assembly/failed")
    public OrderDto returnFailedAssembly(@RequestBody @NotBlank UUID orderId) {
        return orderService.returnFailedAssembly(orderId);
    }
}
