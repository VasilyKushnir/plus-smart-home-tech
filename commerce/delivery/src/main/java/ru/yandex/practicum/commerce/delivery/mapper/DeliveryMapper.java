package ru.yandex.practicum.commerce.delivery.mapper;

import lombok.experimental.UtilityClass;
import ru.yandex.practicum.commerce.delivery.entity.Address;
import ru.yandex.practicum.commerce.delivery.entity.Delivery;
import ru.yandex.practicum.commerce.interactionapi.dto.AddressDto;
import ru.yandex.practicum.commerce.interactionapi.dto.DeliveryDto;

@UtilityClass
public class DeliveryMapper {

    public Address toAddressEntity(AddressDto addressDto) {
        return Address.builder()
                .country(addressDto.getCountry())
                .city(addressDto.getCity())
                .street(addressDto.getStreet())
                .house(addressDto.getHouse())
                .flat(addressDto.getFlat())
                .build();
    }

    public AddressDto toAddressDto(Address address) {
        return AddressDto.builder()
                .country(address.getCountry())
                .city(address.getCity())
                .street(address.getStreet())
                .house(address.getHouse())
                .flat(address.getFlat())
                .build();
    }

    public Delivery toEntity(DeliveryDto deliveryDto) {
        return Delivery.builder()
                .fromAddress(toAddressEntity(deliveryDto.getFromAddress()))
                .toAddress(toAddressEntity(deliveryDto.getToAddress()))
                .orderId(deliveryDto.getOrderId())
                .deliveryState(deliveryDto.getDeliveryState())
                .build();
    }

    public DeliveryDto toDto(Delivery delivery) {
        return DeliveryDto.builder()
                .deliveryId(delivery.getDeliveryId())
                .fromAddress(toAddressDto(delivery.getFromAddress()))
                .toAddress(toAddressDto(delivery.getToAddress()))
                .orderId(delivery.getOrderId())
                .deliveryState(delivery.getDeliveryState())
                .build();
    }
}
