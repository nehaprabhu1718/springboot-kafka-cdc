package com.example.kafkaexample.events.listeners;

import com.example.kafkaexample.kafka.producer.KafkaSendMessage;
import com.example.kafkaexample.token.ResumeTokenManager;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.support.SendResult;

@Component
public class ChangeEventListener {

    private static final Logger logger = LoggerFactory.getLogger(ChangeEventListener.class);

    private final MongoClient mongoClient;
    private final MongoCollection<Document> collection;
    private final ResumeTokenManager resumeTokenManager;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaSendMessage kafkaSendMessage;

    @Autowired
    public ChangeEventListener(MongoClient mongoClient,
                               @Value("${spring.data.mongodb.database}") String database,
                               @Value("${spring.data.mongodb.collection}") String collectionName,
                               ResumeTokenManager resumeTokenManager, KafkaTemplate<String, Object> kafkaTemplate) {

        this.mongoClient = mongoClient;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaSendMessage = new KafkaSendMessage(kafkaTemplate);
        this.collection = mongoClient.getDatabase(database).getCollection(collectionName);
        this.resumeTokenManager = resumeTokenManager;
    }

    public void listenToUpdateChangeEvent() {

        BsonDocument resumeToken = resumeTokenManager.readResumeTokenFromFile();

        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(Filters.in("operationType", List.of("update", "insert")))
        );

        ChangeStreamIterable<Document> changeStream;
        if (resumeToken != null) {
            changeStream = collection.watch(pipeline).resumeAfter(resumeToken)
                    .fullDocument(FullDocument.UPDATE_LOOKUP);
            logger.info("Resuming from token: " + resumeToken.toJson());
        } else {
            changeStream = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
            logger.info("Starting from the beginning as no resume token was found.");
        }

        changeStream.forEach(change -> {
            BsonDocument newResumeToken = change.getResumeToken();
            processChange(change.getFullDocument());

            if (newResumeToken != null) {
                resumeTokenManager.saveResumeTokenToFile(newResumeToken);
                // Removed the duplicate logging here
            } else {
                logger.warn("Change event received without a resume token.");
            }
        });
    }

    public void processChange(Document fullChange) {
        logger.info("Processing change: " + fullChange.toJson());

        CompletableFuture<SendResult<String, Object>> result = null;
        Document messageHeader = new Document("headerKey", "sample header!");

        // Save the document to a file using resumeTokenManager
        resumeTokenManager.saveDocumentToFile(fullChange);

        if (fullChange != null) {
            logger.info("Created message structure & document to pass to Kafka Topic");
            result = kafkaSendMessage.sendMessageToKafka(messageHeader, fullChange);
        } else {
            result = kafkaSendMessage.sendMessageToKafka(null, null);
        }
    }
}
