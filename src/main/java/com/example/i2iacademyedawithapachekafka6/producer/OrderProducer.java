package com.example.i2iacademyedawithapachekafka6.producer;

import com.example.i2iacademyedawithapachekafka6.model.OrderEvent;
import com.example.i2iacademyedawithapachekafka6.serialization.OrderEventSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducer {

    public static final String TOPIC_NAME = "orders-topic";

    public static void main(String[] args) {
        System.out.println("Producer application starting...");

        Properties config = new Properties();
        config.put("client.id", "order-producer-app");
        config.put("bootstrap.servers", "localhost:29092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", OrderEventSerializer.class.getName());

        try (KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(config)) {
            for (int i = 1; i <= 5; i++) {
                String messageKey = "order-key-" + i;
                OrderEvent event = new OrderEvent("ORD-" + i, "Item-" + i, i * 15.0);

                System.out.println("Sending event: " + event);
                producer.send(new ProducerRecord<>(TOPIC_NAME, messageKey, event));
            }
        }

        System.out.println("Producer application finished.");
    }
}
