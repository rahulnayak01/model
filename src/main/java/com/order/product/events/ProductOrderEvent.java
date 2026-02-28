package com.order.product.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProductOrderEvent {
    private String orderId;
    private String productId;
    private BigDecimal price;
    private int quantity;
    private String eventType; // e.g., "ORDER_CREATED", "PAYMENT_PENDING"
}

