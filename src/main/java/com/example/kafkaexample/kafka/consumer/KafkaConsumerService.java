package com.example.kafkaexample.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

@Service
public class KafkaConsumerService {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @KafkaListener(id = "myConsumer", topics = "test_topic", groupId = "springboot-group-1", autoStartup = "true")
    public void consume(Object message) {
        logger.info("Consumed message: {}", message);
        // Process the message as needed
    }
}
