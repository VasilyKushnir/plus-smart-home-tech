package ru.yandex.practicum.commerce.interactionapi.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.data.domain.Page;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.interactionapi.dto.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductReturnRequest;

import java.awt.print.Pageable;
import java.util.UUID;

@FeignClient(name = "order", path = "/api/v1/order")
public interface OrderClient {
    @PutMapping
    OrderDto createNewOrder(@RequestBody CreateNewOrderRequest request);

    @GetMapping
    Page<OrderDto> getOrders(
            @RequestParam String username,
            @PageableDefault(size = 20) Pageable pageable
    );

    @PostMapping("/return")
    OrderDto returnOrder(@RequestBody ProductReturnRequest request);

    @PostMapping("/payment")
    OrderDto payForOrder(@RequestBody UUID orderId);

    @PostMapping("/payment/failed")
    OrderDto returnFailedPayment(@RequestBody UUID orderId);

    @PostMapping("/delivery")
    OrderDto deliverOrder(@RequestBody UUID orderId);

    @PostMapping("/delivery/failed")
    OrderDto returnFailedDelivery(@RequestBody UUID orderId);

    @PostMapping("/completed")
    OrderDto completeOrder(@RequestBody UUID orderId);

    @PostMapping("/calculate/total")
    OrderDto calculateTotal(@RequestBody UUID orderId);

    @PostMapping("/calculate/delivery")
    OrderDto calculateDelivery(@RequestBody UUID orderId);

    @PostMapping("/assembly")
    OrderDto assemblyOrder(@RequestBody UUID orderId);

    @PostMapping("/assembly/failed")
    OrderDto returnFailedAssembly(@RequestBody UUID orderId);
}
