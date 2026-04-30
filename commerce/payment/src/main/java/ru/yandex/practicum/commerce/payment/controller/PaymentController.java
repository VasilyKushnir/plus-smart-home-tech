package ru.yandex.practicum.commerce.payment.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionapi.dto.PaymentDto;
import ru.yandex.practicum.commerce.payment.service.PaymentService;

import java.util.UUID;

@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/payment")
public class PaymentController {
    private final PaymentService paymentService;

    @PostMapping
    public PaymentDto processPayment(@Valid @RequestBody OrderDto order) {
        return paymentService.processPayment(order);
    }

    @PostMapping("/totalCost")
    public double calculateTotal(@Valid @RequestBody OrderDto order) {
        return paymentService.calculateTotal(order);
    }

    @PostMapping("/refund")
    public void refund(@RequestBody @NotBlank UUID paymentId) {
        paymentService.refund(paymentId);
    }

    @PostMapping("/productCost")
    public double calculateProductCost(@Valid @RequestBody OrderDto order) {
        return paymentService.calculateProductCost(order);
    }

    @PostMapping("/failed")
    public void denyPayment(@RequestBody @NotBlank UUID paymentId) {

    }
}
