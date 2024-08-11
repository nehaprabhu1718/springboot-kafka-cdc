package com.example.kafkaexample.config;

import com.mongodb.client.MongoDatabase;
import org.springframework.beans.factory.annotation.Value;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MongoConfig {

    private static final Logger logger = LoggerFactory.getLogger(MongoConfig.class);

    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Value("spring.data.mongodb.database")
    private String mongoDatabase;

    @Value("spring.data.mongodb.collection")
    private String mongoCollection;

    @Bean
    public MongoClient mongoClient() {

        ConnectionString connectionString = new ConnectionString(mongoUri);

        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .build();

        MongoClient mongoClient = MongoClients.create(mongoClientSettings);

        MongoDatabase database = mongoClient.getDatabase(mongoDatabase);
        logger.info("Connected to database: " + database.getName());

        return mongoClient;
    }
}
