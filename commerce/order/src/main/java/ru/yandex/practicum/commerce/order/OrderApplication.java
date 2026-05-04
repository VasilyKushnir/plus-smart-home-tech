package ru.yandex.practicum.commerce.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import ru.yandex.practicum.commerce.interactionapi.feign.DeliveryClient;
import ru.yandex.practicum.commerce.interactionapi.feign.PaymentClient;
import ru.yandex.practicum.commerce.interactionapi.feign.ShoppingCartClient;
import ru.yandex.practicum.commerce.interactionapi.feign.WarehouseClient;

@EnableFeignClients(clients = {
        ShoppingCartClient.class, PaymentClient.class, DeliveryClient.class, WarehouseClient.class})
@SpringBootApplication
public class OrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
