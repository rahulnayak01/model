package com.order.product.service;

import com.order.product.entity.Product;
import com.order.product.events.ProductOrderEvent;
import com.order.product.repo.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;


@Service
public class ProductService {

    private static final Logger logger = LoggerFactory.getLogger(ProductService.class);
    private static final String PRODUCT_TOPIC = "product-orders";
    private static final String INVENTORY_TOPIC = "inventory-events";
    private static final String BULK_MSG_TOPIC = "msg-topic"; // optional second topic

    private final ProductRepository productRepository;
    private final KafkaTemplate<String, ProductOrderEvent> kafkaTemplate;

    public ProductService(ProductRepository productRepository, KafkaTemplate<String, ProductOrderEvent> kafkaTemplate)
 {
        this.productRepository = productRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public Product saveProduct(Product product) {
        Product saved = productRepository.save(product);

        // Publish to multiple topics
        publishProductEvent(saved);

        return saved;
    }

    public Optional<Product> getProductById(String id) {
        return productRepository.findById(id);
    }



    private void publishProductEvent(Product product) {
        ProductOrderEvent event = new ProductOrderEvent();
        event.setOrderId(UUID.randomUUID().toString());
        event.setProductId(product.getProductid());
        event.setPrice(product.getPrice());
        event.setQuantity(product.getQuantity());
        event.setEventType("ORDER_CREATED");

        kafkaTemplate.send("product-orders", product.getProductid(), event);

        kafkaTemplate.send(PRODUCT_TOPIC, product.getProductid(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        logger.info("Sent Product to '{}': {}", PRODUCT_TOPIC, product.getProductid());
                    } else {
                        logger.error("Failed to send to '{}': {}", PRODUCT_TOPIC, ex.getMessage());
                    }
                });

        // Fan-out to another topic (optional)
        kafkaTemplate.send(INVENTORY_TOPIC, product.getProductid(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        logger.info("Sent Product to '{}': {}", INVENTORY_TOPIC, product.getProductid());
                    } else {
                        logger.error("Failed to send to '{}': {}", INVENTORY_TOPIC, ex.getMessage());
                    }
                });
    }



    public void sendProductEventToKafka(Product product) {
        ProductOrderEvent event = new ProductOrderEvent();
        event.setOrderId(UUID.randomUUID().toString());
        event.setProductId(product.getProductid());
        event.setPrice(product.getPrice());
        event.setQuantity(product.getQuantity());
        event.setEventType("ORDER_CREATED");

        kafkaTemplate.send(PRODUCT_TOPIC, product.getProductid(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        logger.info("Sent Product to '{}': {}", PRODUCT_TOPIC, product.getProductid());
                    } else {
                        logger.error("Failed to send to '{}': {}", PRODUCT_TOPIC, ex.getMessage());
                    }
                });
    }

}

