package com.example.kafkaexample.transform;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Map;

@Service
public class DataTransformer {

    private final MongoCollection<Document> collection;

    public DataTransformer(MongoClient mongoClient,
                           @Value("${spring.data.mongodb.database}") String database,
                           @Value("${spring.data.mongodb.sales-changes-collection}") String collectionName) {
        this.collection = mongoClient.getDatabase(database).getCollection(collectionName);
    }

    public void transform(Document document) {
        // Convert $oid to ObjectId
        if (document.containsKey("_id") && document.get("_id") instanceof Map) {
            Map<String, Object> idMap = document.get("_id", Map.class);
            if (idMap.containsKey("$oid")) {
                document.put("_id", new ObjectId((String) idMap.get("$oid")));
            }
        }

        // Convert $date to Date
        if (document.containsKey("saleDate") && document.get("saleDate") instanceof Map) {
            Map<String, Object> dateMap = document.get("saleDate", Map.class);
            if (dateMap.containsKey("$date")) {
                Object dateValue = dateMap.get("$date");
                if (dateValue instanceof Long) {
                    document.put("saleDate", new Date((Long) dateValue));
                } else if (dateValue instanceof String) {
                    // Assuming the date is in ISO-8601 format
                    document.put("saleDate", Date.from(java.time.Instant.parse((String) dateValue)));
                }
            }
        }

        // Additional transformations
        if (document.containsKey("purchaseMethod")) {
            document.put("method_of_purchase", document.remove("purchaseMethod"));
        }

        if (document.containsKey("couponUsed")) {
            document.put("used_coupon", document.remove("couponUsed"));
        }

        if (document.containsKey("storeLocation")) {
            document.put("location", document.remove("storeLocation"));
        }

        // Insert the transformed document into MongoDB
        collection.insertOne(document);
    }
}
