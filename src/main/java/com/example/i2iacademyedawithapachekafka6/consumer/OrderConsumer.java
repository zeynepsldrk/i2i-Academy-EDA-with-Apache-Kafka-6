package com.example.i2iacademyedawithapachekafka6.consumer;

import com.example.i2iacademyedawithapachekafka6.model.OrderEvent;
import com.example.i2iacademyedawithapachekafka6.producer.OrderProducer;
import com.example.i2iacademyedawithapachekafka6.serialization.OrderEventDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {
        System.out.println("Consumer application starting...");

        Properties config = new Properties();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumer-group");
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderEventDeserializer.class.getName());

        try (KafkaConsumer<String, OrderEvent> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList(OrderProducer.TOPIC_NAME));

            while (true) {
                ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, OrderEvent> record : records) {
                    System.out.println("Received -> offset: " + record.offset()
                            + ", key: " + record.key()
                            + ", value: " + record.value());
                }
            }
        }
    }
}
