package com.example.kafkaexample.token;
import com.example.kafkaexample.config.MongoConfig;
import org.bson.BsonDocument;
import org.bson.Document;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

public class ResumeTokenManager {

    private static final Logger logger = LoggerFactory.getLogger(ResumeTokenManager.class);

    public String resumeTokenPath;

    public ResumeTokenManager(@Value("${resume.token.filePath}") String tokenPath) {
        this.resumeTokenPath = tokenPath;
    }

    public BsonDocument readResumeTokenFromFile() {
        try (BufferedReader reader = new BufferedReader(new FileReader(resumeTokenPath))) {
            String tokenJson = reader.readLine();
            return BsonDocument.parse(tokenJson);
        } catch (IOException e) {
            logger.info("No resume token found. Starting from the beginning.");
        }
        return null;
    }

    public void saveDocumentToFile(Document document) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("latestDocument.txt", true))) {
            writer.write(document.toJson() + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void saveResumeTokenToFile(BsonDocument resumeToken) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resumeTokenPath))) {
            writer.write(resumeToken.toJson());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
