package com.kafkaImpl.KafkaImpl.controller;

//import com.kafkaImpl.KafkaImpl.service.KafkaConsumer;
import com.kafkaImpl.KafkaImpl.service.KafkaConsumerManager;

import com.kafkaImpl.KafkaImpl.service.KafkaProducer;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

        import java.util.concurrent.CompletableFuture;



@RestController
@RequestMapping("/api/kafka")
@AllArgsConstructor
public class KafkaController {

    @Autowired
    private final KafkaProducer producer;

   @Autowired
   private final KafkaConsumerManager consumerManager;

    @PostMapping("/send/{userId}/{message}")
    public CompletableFuture<ResponseEntity<String>> sendMessage(@PathVariable String userId, @PathVariable String message) {
        return producer.sendMessage(userId, message);
    }

    @PostMapping("/start")
    public String startConsumers() {
        consumerManager.startConsumers();
        return "Kafka consumers started.";
    }

    @PostMapping("/stop")
    public String stopConsumers() {
        consumerManager.stopConsumers();
        return "Kafka consumers stopped.";
    }

//    @GetMapping("/consume")
//    public String consumeMessages() {
//        new Thread(kafkaConsumerService::consumeMessages).start();
//        return "Kafka consumer started.";
//    }

//    @GetMapping("/consume")
//    public String consume() {
//        // Run in a separate thread as consumer can run on infinite loop so user wont receive a message
//        new Thread(kafkaConsumerService::consumeMessages).start();
//        return "Kafka Consumer Started!";
//    }

//    @GetMapping("/consume/{userId}/{maxMessages}")
//    public ResponseEntity<?> consumeUserMessages(
//            @PathVariable String userId,
//            @PathVariable int maxMessages) {
//
//        int partition = Math.abs(userId.hashCode() % 3);
//        try {
//            List<String> messages = kafkaConsumer.getMessagesForUser(userId, maxMessages);
//            return ResponseEntity.ok(new KafkaResponse(userId, partition, messages));
//        } catch (Exception e) {
//            return ResponseEntity.status(500).body("Error fetching messages: " + e.getMessage());
//        }
//    }
}
