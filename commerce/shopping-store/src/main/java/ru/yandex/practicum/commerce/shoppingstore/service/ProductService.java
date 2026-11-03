package ru.yandex.practicum.commerce.shoppingstore.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductCategory;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductDto;
import ru.yandex.practicum.commerce.interactionapi.dto.UpdateProductQuantityRequest;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface ProductService {
    ProductDto getProductById(UUID id);

    Map<UUID, ProductDto> getProductByIds(List<UUID> productIds);

    Page<ProductDto> getProductsByCategory(ProductCategory category, Pageable pageable);

    ProductDto addProduct(ProductDto productDto);

    ProductDto updateProduct(ProductDto productDto);

    boolean deleteProduct(UUID id);

    boolean updateQuantity(UpdateProductQuantityRequest request);
}
