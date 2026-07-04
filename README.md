# Event-Driven Architecture (EDA) with Apache Kafka

This project was developed as part of the i2i Academy internship to understand the principles of **Event-Driven Architecture (EDA)** and to build asynchronous communication systems using Apache Kafka.

## Project Objective
The main goal of this project is to transition from a tightly coupled, classical Request-Response architecture to a loosely coupled architecture where services communicate asynchronously by publishing and subscribing to "Events" without waiting for each other.

## Technologies and Tools
- Java 17+
- Apache Kafka (KRaft Mode)
- Docker & Docker Compose
- Maven
- Spring Boot (for project infrastructure)

## Project Architecture
The project consists of two core components that do not communicate directly but only interact through Apache Kafka:

1. Producer (OrderProducer.java): When a new order is created, it publishes an `OrderEvent` object to the `orders-topic` in Kafka.
2. Consumer (OrderConsumer.java): Listens to the `orders-topic`. When a new event arrives, it consumes the event, deserializes it back into a Java object, and processes it.
3. Serialization/Deserialization: For data transmission over the network, Java objects are serialized into byte arrays (`OrderEventSerializer`) and deserialized back into objects on the receiver side (`OrderEventDeserializer`).

## Setup and Execution

### 1. Starting the Kafka Broker
This project uses the modern KRaft architecture, which eliminates the need for Zookeeper. To start Kafka, run the following command in the root directory of the project:
```bash
docker compose up -d
```
(You can verify that Kafka is running using the `docker ps` command. Kafka listens to the outside world on port `29092`.)

### 2. Compiling the Project
Before running the applications, compile the project using Maven:
```bash
./mvnw clean compile
```

### 3. Running the Consumer
To start listening and catching orders, launch the Consumer in your terminal:
```bash
./mvnw exec:java -Dexec.mainClass="com.example.i2iacademyedawithapachekafka6.consumer.OrderConsumer"
```

### 4. Running the Producer
In a new terminal window (or a split screen using tmux), launch the Producer to send orders to the broker:
```bash
./mvnw exec:java -Dexec.mainClass="com.example.i2iacademyedawithapachekafka6.producer.OrderProducer"
```

## Internship Notes and Learning Outcomes
- Isolation and Port Forwarding: Understood the distinction between Kafka's internal (9092) and external (29092) ports using Docker.
- Serialization Logic: Observed that systems communicate via bytes, not objects, and implemented custom deserializer/serializer classes using a manual delimiter (`#`).
- Load Balancing: Learned how messages can be distributed across different consumers using the `GROUP_ID` concept.
- Preventing Data Loss: Ensured that even if the system crashes, old messages can be reread (replayed) from Kafka using the `AUTO_OFFSET_RESET_CONFIG="earliest"` configuration.
