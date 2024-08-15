package com.example.kafkaexample.token;

import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

@Component
public class ResumeTokenManager {

    private static final Logger logger = LoggerFactory.getLogger(ResumeTokenManager.class);

    private final String resumeTokenPath;

    public ResumeTokenManager(@Value("${resume.token.filePath}") String tokenPath) {
        this.resumeTokenPath = tokenPath;  // Ensure this path points to "token.txt"
    }

    public BsonDocument readResumeTokenFromFile() {
        try (BufferedReader reader = new BufferedReader(new FileReader(resumeTokenPath))) {
            String tokenJson = reader.readLine();
            if (tokenJson != null && !tokenJson.isEmpty()) {
                return BsonDocument.parse(tokenJson);
            }
        } catch (IOException e) {
            logger.info("No resume token found or failed to read the token. Starting from the beginning.");
        }
        return null;
    }

    public void saveDocumentToFile(Document document) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("latestDocument.txt", true))) {
            writer.write(document.toJson() + "\n");
        } catch (IOException e) {
            logger.error("Failed to save document to file.", e);
        }
    }

    public void saveResumeTokenToFile(BsonDocument resumeToken) {
        if (resumeToken != null) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(resumeTokenPath, false))) { // 'false' to overwrite
                writer.write(resumeToken.toJson());
                logger.info("Resume token saved: " + resumeToken.toJson());
            } catch (IOException e) {
                logger.error("Failed to save resume token to file.", e);
            }
        } else {
            logger.warn("Attempted to save a null resume token.");
        }
    }
}
