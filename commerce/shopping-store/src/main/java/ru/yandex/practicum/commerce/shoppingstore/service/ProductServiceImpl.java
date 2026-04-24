package ru.yandex.practicum.commerce.shoppingstore.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductDto;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductCategory;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductState;
import ru.yandex.practicum.commerce.interactionapi.dto.UpdateProductQuantityRequest;
import ru.yandex.practicum.commerce.shoppingstore.entity.Product;
import ru.yandex.practicum.commerce.shoppingstore.exception.NotFoundException;
import ru.yandex.practicum.commerce.shoppingstore.mapper.ProductMapper;
import ru.yandex.practicum.commerce.shoppingstore.repository.ProductRepository;

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
    public Page<ProductDto> getProductsByCategory(ProductCategory category, Pageable pageable) {
        Page<Product> products = productRepository.findByProductCategory(category, pageable);
        return products.map(ProductMapper::toDto);
    }

    @Override
    public ProductDto addProduct(ProductDto productDto) {
        Product product = ProductMapper.toEntity(productDto);
        product = productRepository.save(product);
        return ProductMapper.toDto(product);
    }

    @Override
    public ProductDto updateProduct(ProductDto productDto) {
        UUID id = productDto.getProductId();
        Product currentProduct = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product with id " + id + " was not found"));
        Product updatedProduct = ProductMapper.updateProductFields(currentProduct, productDto);
        return ProductMapper.toDto(productRepository.save(updatedProduct));
    }

    @Override
    public boolean deleteProduct(UUID id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product with id " + id + " was not found"));
        product.setProductState(ProductState.DEACTIVATE);
        productRepository.save(product);
        return true;
    }

    @Override
    public boolean updateQuantity(UpdateProductQuantityRequest request) {
        UUID id = request.getProductId();
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product with id " + id + " was not found"));
        product.setQuantityState(request.getQuantityState());
        productRepository.save(product);
        return true;
    }
}
