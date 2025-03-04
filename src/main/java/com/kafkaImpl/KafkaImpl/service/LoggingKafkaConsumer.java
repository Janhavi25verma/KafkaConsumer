package com.kafkaImpl.KafkaImpl.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
import java.util.Map;


public class LoggingKafkaConsumer extends AbstractKafkaConsumer {

    public LoggingKafkaConsumer(String topic, Map<Integer, Integer> partitionLimits) {
        super(topic, partitionLimits);
    }

    @Override
    protected void processMessage(ConsumerRecord<String, String> record) {
        System.out.println("Consumed from partition " + record.partition() + ": " + record.value());
    }
}


//package com.kafkaImpl.KafkaImpl.service;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.springframework.kafka.core.ConsumerFactory;
//import org.springframework.stereotype.Component;
//
//import java.util.Map;
//
//@Component
//public class LoggingKafkaConsumer extends AbstractKafkaConsumer {
//
//    public LoggingKafkaConsumer(ConsumerFactory<String, String> consumer, String topic, Map<Integer, Integer> partitionLimits) {
//        super(consumer, topic, partitionLimits);
//    }
//
//    // Correctly Implementing the Abstract Method
//    @Override
//    protected void processMessage(ConsumerRecord<String, String> record) {
//        processMessage(record.value(), record.partition());  // Call the other method
//    }
//
//    // Helper method for processing messages
//    protected void processMessage(String message, int partition) {
//        System.out.println("Consumed from partition " + partition + ": " + message);
//    }
//}
