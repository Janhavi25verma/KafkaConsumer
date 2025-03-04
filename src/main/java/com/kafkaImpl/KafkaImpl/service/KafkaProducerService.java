package com.kafkaImpl.KafkaImpl.service;

import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@AllArgsConstructor
public class KafkaProducerService {

    @Autowired
    private final KafkaTemplate<String, String> kafkaTemplate;

    // Send message to a specific partition based on user ID
    public CompletableFuture<ResponseEntity<String>> sendMessage(String userId, String message) {
        int partition = getPartition(userId);
        //make it asyncronous to get result in future
       CompletableFuture<SendResult<String,String>> future = kafkaTemplate.send("Users", partition, userId, message);
        return future.handle((result, e) -> {
            //returns relevant message for success and failiur
            if (e == null) {
                return ResponseEntity.ok("Sent message: [" + message + "]" +
                        " with offset: [" + result.getRecordMetadata().offset() + "]");//prints sent message and partiotion where message is sent
            } else {
                return ResponseEntity.status(500).body("Unable to send message: [" + message + "] due to: " + e.getMessage());
            }
        });
    }

    //PUBLISH multiple messages for 3 user in 3 partition for testing
    public void sendMessages(String userId, int count) {
        int partition = getPartition(userId); // Use custom hashing for partitioning
        for (int i = 1; i <= count; i++) {
            String message = "Test message " + i + " for " + userId;
            kafkaTemplate.send("Users", partition, userId, message);
        }
    }


    // Partitioning logic (hash-based)
    //if we randomly assign partition it can go to different partition
    //we want a paerticular user data to be in one partition
    //hashing maps userId to same partition only
    private int getPartition(String userId) {
        return Math.abs(userId.hashCode() % 3); // Assuming 3 partitions
    }

}
