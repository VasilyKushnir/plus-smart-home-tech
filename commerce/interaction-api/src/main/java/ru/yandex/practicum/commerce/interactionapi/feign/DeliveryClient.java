package ru.yandex.practicum.commerce.interactionapi.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.commerce.interactionapi.dto.DeliveryDto;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;

import java.util.UUID;

@FeignClient(name = "delivery", path = "/api/v1/delivery")
public interface DeliveryClient {

    @PutMapping
    DeliveryDto addDelivery(@RequestBody DeliveryDto deliveryDto);

    @PostMapping("/successful")
    void returnSuccessfulDelivery(@RequestBody UUID orderId);

    @PostMapping("/picked")
    void returnPickedDelivery(@RequestBody UUID orderId);

    @PostMapping("/failed")
    void returnFailedDelivery(@RequestBody UUID orderId);

    @PostMapping("/cost")
    double returnDeliveryCost(@RequestBody OrderDto orderDto);
}
