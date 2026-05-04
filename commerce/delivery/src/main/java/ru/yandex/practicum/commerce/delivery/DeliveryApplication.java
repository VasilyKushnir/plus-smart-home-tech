package ru.yandex.practicum.commerce.delivery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import ru.yandex.practicum.commerce.interactionapi.feign.OrderClient;
import ru.yandex.practicum.commerce.interactionapi.feign.WarehouseClient;

@EnableFeignClients(clients = {
        OrderClient.class, WarehouseClient.class})
@SpringBootApplication
public class DeliveryApplication {
    public static void main(String[] args) {
        SpringApplication.run(DeliveryApplication.class, args);
    }
}
