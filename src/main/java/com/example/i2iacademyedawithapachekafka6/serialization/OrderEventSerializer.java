package com.example.i2iacademyedawithapachekafka6.serialization;

import com.example.i2iacademyedawithapachekafka6.model.OrderEvent;
import org.apache.kafka.common.serialization.Serializer;
import java.nio.charset.StandardCharsets;

public class OrderEventSerializer implements Serializer<OrderEvent> {

    private static final String DELIMITER = "#";

    @Override
    public byte[] serialize(String topic, OrderEvent event) {
        if (event == null) {
            return null;
        }
        String payload = event.getOrderId() + DELIMITER + event.getProductName() + DELIMITER + event.getAmount();
        return payload.getBytes(StandardCharsets.UTF_8);
    }
}
