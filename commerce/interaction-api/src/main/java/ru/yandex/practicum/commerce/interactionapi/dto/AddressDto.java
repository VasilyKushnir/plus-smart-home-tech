package ru.yandex.practicum.commerce.interactionapi.dto;

import lombok.Builder;

@lombok.Value
@Builder(toBuilder = true)
public class AddressDto {
    String country;
    String city;
    String street;
    String house;
    String flat;
}
