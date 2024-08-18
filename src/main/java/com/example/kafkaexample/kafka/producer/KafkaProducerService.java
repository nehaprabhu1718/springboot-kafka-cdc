package com.example.kafkaexample.kafka.producer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.CompletableFuture;

public class KafkaProducerService {

    public static final Logger logger = LogManager.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, Object>> sendMessageToKafka(Document headerValue, Document document) {

        String message = document.toJson();
        CompletableFuture<SendResult<String, Object>> completableFuture = new CompletableFuture<>();

        String topic = "test_topic";

        Message<String> kafkaMessageHeader = MessageBuilder.withPayload(message)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader("customerHeader", headerValue)
                .build();

        logger.info("Headers getting added: {}", kafkaMessageHeader);

        kafkaTemplate.send(kafkaMessageHeader).whenComplete((result, ex) -> {
            if (ex == null) {
                RecordMetadata metadata = result.getRecordMetadata();
                logger.info("Sent message=[{}] with offset=[{}] and partition=[{}]", message, metadata.offset(), metadata.partition());
                completableFuture.complete(result);
            } else {
                logger.error("Unable to send message= {} due to {}", message, ex.getMessage());
                completableFuture.completeExceptionally(ex);
            }
        });
        return completableFuture;
    }

}

