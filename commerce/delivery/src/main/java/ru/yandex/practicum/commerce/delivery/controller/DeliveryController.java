package ru.yandex.practicum.commerce.delivery.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.delivery.service.DeliveryService;
import ru.yandex.practicum.commerce.interactionapi.dto.DeliveryDto;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;

import java.util.UUID;

@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/delivery")
public class DeliveryController {
    private final DeliveryService deliveryService;

    @PutMapping
    public DeliveryDto addDelivery(@Valid @RequestBody DeliveryDto deliveryDto) {
        return deliveryService.addDelivery(deliveryDto);
    }

    @PostMapping("/successful")
    public void returnSuccessfulDelivery(@RequestBody @NotBlank UUID orderId) {
        deliveryService.returnSuccessfulDelivery(orderId);
    }

    @PostMapping("/picked")
    public void returnPickedDelivery(@RequestBody @NotBlank UUID orderId) {
        deliveryService.returnPickedDelivery(orderId);
    }

    @PostMapping("/failed")
    public void returnFailedDelivery(@RequestBody @NotBlank UUID orderId) {
        deliveryService.returnFailedDelivery(orderId);
    }

    @PostMapping("/cost")
    public double returnDeliveryCost(@Valid @RequestBody OrderDto orderDto) {
        return deliveryService.returnDeliveryCost(orderDto);
    }
}
