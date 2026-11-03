package ru.yandex.practicum.commerce.shoppingstore.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductCategory;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductDto;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductState;
import ru.yandex.practicum.commerce.interactionapi.dto.UpdateProductQuantityRequest;
import ru.yandex.practicum.commerce.shoppingstore.entity.Product;
import ru.yandex.practicum.commerce.shoppingstore.exception.NotFoundException;
import ru.yandex.practicum.commerce.shoppingstore.mapper.ProductMapper;
import ru.yandex.practicum.commerce.shoppingstore.repository.ProductRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ProductServiceImpl implements ProductService {
    private final ProductRepository productRepository;

    @Override
    public ProductDto getProductById(UUID id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product with id " + id + " was not found"));
        return ProductMapper.toDto(product);
    }

    @Override
    public Map<UUID, ProductDto> getProductByIds(List<UUID> productIds) {
        List<Product> products = productRepository.findAllById(productIds);

        if (products.size() != productIds.size()) {
            throw new NotFoundException("Some products were not found");
        }

        Map<UUID, ProductDto> map = new HashMap<>();

        for (Product product : products) {
            ProductDto productDto = ProductMapper.toDto(product);
            map.put(productDto.getProductId(), productDto);
        }

        return map;
    }

    @Override
    public Page<ProductDto> getProductsByCategory(ProductCategory category, Pageable pageable) {
        Page<Product> products = productRepository.findByProductCategory(category, pageable);
        return products.map(ProductMapper::toDto);
    }

    @Override
    @Transactional
    public ProductDto addProduct(ProductDto productDto) {
        Product product = ProductMapper.toEntity(productDto);
        product = productRepository.save(product);
        return ProductMapper.toDto(product);
    }

    @Override
    @Transactional
    public ProductDto updateProduct(ProductDto productDto) {
        UUID id = productDto.getProductId();
        Product currentProduct = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product with id " + id + " was not found"));
        Product updatedProduct = ProductMapper.updateProductFields(currentProduct, productDto);
        return ProductMapper.toDto(productRepository.save(updatedProduct));
    }

    @Override
    @Transactional
    public boolean deleteProduct(UUID id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product with id " + id + " was not found"));
        product.setProductState(ProductState.DEACTIVATE);
        productRepository.save(product);
        return true;
    }

    @Override
    @Transactional
    public boolean updateQuantity(UpdateProductQuantityRequest request) {
        UUID id = request.getProductId();
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product with id " + id + " was not found"));
        product.setQuantityState(request.getQuantityState());
        productRepository.save(product);
        return true;
    }
}
