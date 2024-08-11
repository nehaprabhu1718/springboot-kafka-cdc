package com.example.kafkaexample.events.listeners;

import com.example.kafkaexample.config.MongoConfig;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.bson.Document;

public class UpdateEventListener {

    @Value("${spring.data.mongodb.database}")
    final public String database;

    @Value("${spring.data.mongodb.collection}")
    final public String collectionName;

    final public MongoCollection<Document> collection;

    public UpdateEventListener(final MongoClient mongoClient, final MongoTemplate mongoTemplate, String database, String collectionName) {
        this.database = database;
        this.collectionName = collectionName;

        this.mongoClient = mongoClient;
        this.mongoTemplate = mongoTemplate;

        this.collection = mongoClient.getDatabase(database).getCollection(collectionName);
    }


}
