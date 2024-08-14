package com.example.kafkaexample.events.listeners;

import com.example.kafkaexample.config.MongoConfig;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.conversions.Bson;
import java.util.List;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.bson.Document;

import java.util.Arrays;

public class UpdateEventListener {

    @Value("${spring.data.mongodb.database}")
    final public String database;

    @Value("${spring.data.mongodb.collection}")
    final public String collectionName;

    @Autowired
    final public MongoClient mongoClient;

    final public MongoCollection<Document> collection;

    public UpdateEventListener(final MongoClient mongoClient, String database, String collectionName) {
        this.database = database;
        this.collectionName = collectionName;

        this.mongoClient = mongoClient;
        this.collection = mongoClient.getDatabase(database).getCollection(collectionName);
    }

    public void listenToChangeEvent() {

        List<Bson> pipeline = Arrays.asList(
            Aggregates.match(Filters.in("operationType", Arrays.asList("insert", "update", "replace")))
        );

        ChangeStreamIterable<Document> changeStream = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);

        changeStream.forEach(change -> {
            processChange(change.getFullDocument());
        });
    }

    public void processChange(Document fullChange) {
        System.out.println(fullChange);
    }

}
