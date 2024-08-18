package com.example.kafkaexample.kafka.consumer;

import com.example.kafkaexample.transform.DataTransformer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class KafkaConsumerService {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final DataTransformer dataTransformer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaConsumerService(DataTransformer dataTransformer) {
        this.dataTransformer = dataTransformer;
    }

    @KafkaListener(id = "myConsumer", topics = "test_topic", groupId = "springboot-group-1", autoStartup = "true")
    public void consume(ConsumerRecord<String, LinkedHashMap<String, Object>> record) {
        logger.info("Consumed message: {}", record.value());

        LinkedHashMap<String, Object> message = record.value();

        // Convert LinkedHashMap to BSON Document
        Document document = new Document(message);

        // Transform the document and save to MongoDB
        dataTransformer.transform(document);
        logger.info("Document transformed and inserted into the database.");
    }
}
