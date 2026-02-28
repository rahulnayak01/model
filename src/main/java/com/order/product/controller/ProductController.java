package com.order.product.controller;

import com.order.product.entity.Product;
import com.order.product.service.ProductService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/store")
public class ProductController {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @PostMapping("/products")
    public Product createProduct(@RequestBody Product product) {

        return productService.saveProduct(product);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Product> getProduct(@PathVariable String id) {
        return productService.getProductById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }


    @PostMapping("/event")
    public ResponseEntity<String> sendEvent(@RequestBody Product product) {
        productService.sendProductEventToKafka(product);
        return ResponseEntity.ok("Product event sent to Kafka successfully!");
    }

}
