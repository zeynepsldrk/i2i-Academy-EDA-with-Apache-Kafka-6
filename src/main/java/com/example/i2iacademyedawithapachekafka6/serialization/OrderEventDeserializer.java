package com.example.i2iacademyedawithapachekafka6.serialization;

import com.example.i2iacademyedawithapachekafka6.model.OrderEvent;
import org.apache.kafka.common.serialization.Deserializer;
import java.nio.charset.StandardCharsets;

public class OrderEventDeserializer implements Deserializer<OrderEvent> {

    private static final String DELIMITER = "#";

    @Override
    public OrderEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        String payload = new String(data, StandardCharsets.UTF_8);
        String[] fields = payload.split(DELIMITER);

        String orderId = fields[0];
        String productName = fields[1];
        double amount = Double.parseDouble(fields[2]);

        return new OrderEvent(orderId, productName, amount);
    }
}
