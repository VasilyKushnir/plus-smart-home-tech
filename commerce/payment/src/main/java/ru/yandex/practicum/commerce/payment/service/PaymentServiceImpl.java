package ru.yandex.practicum.commerce.payment.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.commerce.interactionapi.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionapi.dto.PaymentDto;
import ru.yandex.practicum.commerce.interactionapi.dto.PaymentStatus;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductDto;
import ru.yandex.practicum.commerce.interactionapi.feign.OrderClient;
import ru.yandex.practicum.commerce.interactionapi.feign.ShoppingStoreClient;
import ru.yandex.practicum.commerce.payment.entity.Payment;
import ru.yandex.practicum.commerce.payment.exception.NoPaymentFoundException;
import ru.yandex.practicum.commerce.payment.exception.NotEnoughInfoInOrderToCalculateException;
import ru.yandex.practicum.commerce.payment.mapper.PaymentMapper;
import ru.yandex.practicum.commerce.payment.repository.PaymentRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentServiceImpl implements PaymentService {
    PaymentRepository paymentRepository;
    ShoppingStoreClient shoppingStoreClient;
    OrderClient orderClient;

    @Override
    public PaymentDto processPayment(OrderDto order) {
        double feeTotal = order.getTotalPrice()
                - order.getProductPrice()
                - order.getDeliveryPrice();

        Payment payment = PaymentMapper.fromOrderDtoToPayment(order, feeTotal);
        payment = paymentRepository.save(payment);

        return PaymentMapper.toDto(payment);
    }

    @Override
    public double calculateTotal(OrderDto order) {
        if (order.getDeliveryPrice() == null) {
            throw new NotEnoughInfoInOrderToCalculateException("Not enough info in order to calculate");
        }

        return (order.getProductPrice() * 0.1)
                + order.getProductPrice()
                + order.getDeliveryPrice();
    }

    @Override
    public void successPayment(UUID paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NoPaymentFoundException("Payment with id: " + paymentId + " was not found"));
        payment.setPaymentStatus(PaymentStatus.SUCCESS);
        log.info("Calling orderClient.payForOrder for OrderId: {}", payment.getOrderId());
        orderClient.payForOrder(payment.getOrderId());
    }

    @Override
    public double calculateProductCost(OrderDto order) {
        Map<UUID, Integer> products = order.getProducts();
        List<UUID> productIds = new ArrayList<>(products.keySet());
        log.info("Calling shoppingStoreClient.getProductByIds for orderId: {}", order.getOrderId());
        log.debug("Product Ids: {}", productIds);
        Map<UUID, ProductDto> productMap = shoppingStoreClient.getProductByIds(productIds);
        log.debug("Product Map: {}", productMap);

        double totalProductCost = 0.0;

        for (Map.Entry<UUID, Integer> entry : products.entrySet()) {
            UUID productId = entry.getKey();
            int quantity = entry.getValue();

            ProductDto product = productMap.get(productId);
            double productCost = product.getPrice();

            totalProductCost += productCost * quantity;
        }
        return totalProductCost;
    }

    @Override
    public void denyPayment(UUID paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NoPaymentFoundException("Payment with id: " + paymentId + " was not found"));
        payment.setPaymentStatus(PaymentStatus.FAILED);
        log.info("Calling orderClient.returnFailedPayment for OrderId: {}", payment.getOrderId());
        orderClient.returnFailedPayment(payment.getOrderId());
    }
}
