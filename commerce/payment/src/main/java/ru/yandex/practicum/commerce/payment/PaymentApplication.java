package ru.yandex.practicum.commerce.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import ru.yandex.practicum.commerce.interactionapi.feign.OrderClient;
import ru.yandex.practicum.commerce.interactionapi.feign.ShoppingStoreClient;

@EnableFeignClients(clients = {
        ShoppingStoreClient.class, OrderClient.class})
@SpringBootApplication
public class PaymentApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentApplication.class, args);
    }
}
