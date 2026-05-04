package ru.yandex.practicum.commerce.shoppingstore.contoller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductCategory;
import ru.yandex.practicum.commerce.interactionapi.dto.ProductDto;
import ru.yandex.practicum.commerce.interactionapi.dto.UpdateProductQuantityRequest;
import ru.yandex.practicum.commerce.shoppingstore.service.ProductService;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/shopping-store")
public class ProductController {
    private final ProductService productService;

    @GetMapping("/{productId}")
    public ProductDto getProductById(@PathVariable UUID productId) {
        return productService.getProductById(productId);
    }

    @PostMapping("/batch")
    public Map<UUID, ProductDto> getProductByIds(@RequestBody @NotEmpty List<UUID> productIds) {
        return productService.getProductByIds(productIds);
    }

    @GetMapping
    public Page<ProductDto> getProductsByCategory(
            @RequestParam ProductCategory category,
            @PageableDefault(size = 20) Pageable pageable
    ) {
        return productService.getProductsByCategory(category, pageable);
    }

    @PutMapping
    public ProductDto addProduct(@Valid @RequestBody ProductDto productDto) {
        return productService.addProduct(productDto);
    }

    @PostMapping
    public ProductDto updateProduct(@Valid @RequestBody ProductDto productDto) {
        return productService.updateProduct(productDto);
    }

    @PostMapping("/removeProductFromStore")
    public boolean deleteProduct(@RequestBody @NotNull UUID id) {
        return productService.deleteProduct(id);
    }

    @PostMapping("/quantityState")
    public boolean updateQuantity(UpdateProductQuantityRequest request) {
        return productService.updateQuantity(request);
    }
}
