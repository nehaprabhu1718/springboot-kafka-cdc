package com.example.kafkaexample.events.listeners;

import com.example.kafkaexample.config.MongoConfig;
import com.example.kafkaexample.token.ResumeTokenManager;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.conversions.Bson;
import java.util.List;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class UpdateEventListener {

    private static final Logger logger = LoggerFactory.getLogger(UpdateEventListener.class);

    final public MongoClient mongoClient;
    final public MongoCollection<Document> collection;
    ResumeTokenManager resumeTokenManager;

    @Autowired
    public UpdateEventListener(MongoClient mongoClient,
                               @Value("${spring.data.mongodb.database}") String database,
                               @Value("${spring.data.mongodb.collection}") String collectionName) {

        this.mongoClient = mongoClient;
        this.collection = mongoClient.getDatabase(database).getCollection(collectionName);
    }

    public void listenToChangeEvent() {

        List<Bson> pipeline = Arrays.asList(
            Aggregates.match(Filters.in("operationType", Arrays.asList("insert", "update", "replace")))
        );

        ChangeStreamIterable<Document> changeStream = (resumeTokenManager != null) ?
                collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).resumeAfter(resumeTokenManager.readResumeTokenFromFile()) :
                collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);

        changeStream.forEach(change -> {
            processChange(change.getFullDocument());

            if (resumeTokenManager != null) {
                resumeTokenManager.saveResumeTokenToFile(change.getResumeToken());
            }
        });
    }

    public void processChange(Document fullChange) {
        System.out.println(fullChange);
        resumeTokenManager.saveDocumentToFile(fullChange);
    }

}
