package com.kafkaImpl.KafkaImpl.service;

import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class KafkaConsumerManager {

    // List to track active consumers
    private final List<AbstractKafkaConsumer> consumers = new ArrayList<>();
    private final List<Thread> consumerThreads = new ArrayList<>();

    public void startConsumers() {
        String topic = "Users";

        // Define partition limits
        Map<Integer, Integer> partitionLimits1 = Map.of(0, 4);
        Map<Integer, Integer> partitionLimits2 = Map.of(1, 3);
        Map<Integer, Integer> partitionLimits3 = Map.of(2, 5);

        // Create consumer instances
        LoggingKafkaConsumer consumer1 = new LoggingKafkaConsumer(topic, partitionLimits1);
        LoggingKafkaConsumer consumer2 = new LoggingKafkaConsumer(topic, partitionLimits2);
        LoggingKafkaConsumer consumer3 = new LoggingKafkaConsumer(topic, partitionLimits3);

        consumers.add(consumer1);
        consumers.add(consumer2);
        consumers.add(consumer3);

        // Create and manage threads with unique names
        createOrRestartThread(consumer1, 0);
        createOrRestartThread(consumer2, 1);
        createOrRestartThread(consumer3, 2);



        System.out.println("Started 3 Kafka Consumers, each assigned to a specific partition.");
    }
   //CHECKS if thread is already running then does not create or restart that thread
    //if thread is not running creates thread with a unique name
   private void createOrRestartThread(AbstractKafkaConsumer consumer, int consumerIndex) {
       Thread consumerThread = new Thread(consumer);

       // Assign a unique name to the thread using the consumer index
       consumerThread.setName("Consumer-" + consumer.getClass().getSimpleName() + "-" + consumerIndex);

       // Check if a thread is already running for this consumer
       boolean isThreadRunning = consumerThreads.stream()
               .anyMatch(thread -> thread.getName().equals(consumerThread.getName()) && thread.isAlive());

       if (!isThreadRunning) {
           consumerThreads.add(consumerThread);
           consumerThread.start();
       }
   }
    //HERE THREAD IS NAMED AS CLASS NAME BUT ALL THREEE THREADS ARE USING SAME CLASS SO WONT WORK
//    private void createOrRestartThread(AbstractKafkaConsumer consumer) {
//        Thread consumerThread = new Thread(consumer);
//
//        // Check if a thread is already running for this consumer
//        boolean isThreadRunning = consumerThreads.stream()
//                .anyMatch(thread -> thread.getName().equals(consumer.getClass().getSimpleName()) && thread.isAlive());
//
//        if (!isThreadRunning) {
//            consumerThreads.add(consumerThread);
//            consumerThread.start();
//        }
//    }

    public void stopConsumers() {
        consumers.forEach(AbstractKafkaConsumer::stop);


        //iterating over consumer threads
        for (Thread thread : consumerThreads) {
            try {
                thread.join();//join makes calling thread wait
                // for current thread to finish before proceeding
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        consumers.clear();
        consumerThreads.clear();

        System.out.println("All Kafka consumers and threads stopped.");
    }
}





//MULTIPLE CONSUMER BUT RESTARTS WITHOUT CHECKING IF THREAD IS ALREADY RUNNING
//package com.kafkaImpl.KafkaImpl.service;
//
//import org.springframework.stereotype.Service;
//import java.util.*;
//
//@Service
//public class KafkaConsumerManager {
//
//    // List to track active consumers
//    private final List<AbstractKafkaConsumer> consumers = new ArrayList<>();
//    private final List<Thread> consumerThreads = new ArrayList<>();
//
//    public void startConsumers() {
//        String topic = "Users";
//
//        // Define partition limits
//        Map<Integer, Integer> partitionLimits1 = Map.of(0, 4);
//        Map<Integer, Integer> partitionLimits2 = Map.of(1, 3);
//        Map<Integer, Integer> partitionLimits3 = Map.of(2, 5);
//
//        // Create consumer instances
//        LoggingKafkaConsumer consumer1 = new LoggingKafkaConsumer(topic, partitionLimits1);
//        LoggingKafkaConsumer consumer2 = new LoggingKafkaConsumer(topic, partitionLimits2);
//        LoggingKafkaConsumer consumer3 = new LoggingKafkaConsumer(topic, partitionLimits3);
//
//        consumers.add(consumer1);
//        consumers.add(consumer2);
//        consumers.add(consumer3);
//
//        // Create and start threads
//        Thread thread1 = new Thread(consumer1);
//        Thread thread2 = new Thread(consumer2);
//        Thread thread3 = new Thread(consumer3);
//
//        consumerThreads.add(thread1);
//        consumerThreads.add(thread2);
//        consumerThreads.add(thread3);
//
//
//        consumerThreads.forEach(Thread::start);
////        thread1.start();
////        thread2.start();
////        thread3.start();
//
//        System.out.println("Started 3 Kafka Consumers, each assigned to a specific partition.");
//    }
//
//    public void stopConsumers() {
//        consumers.forEach(AbstractKafkaConsumer::stop);
//
//        for (Thread thread : consumerThreads) {
//            try {
//                thread.join();
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//        }
//
//        consumers.clear();
//        consumerThreads.clear();
//
//        System.out.println("All Kafka consumers stopped.");
//    }
//}
//
//







//SINGLE CONSUMER

//package com.kafkaImpl.KafkaImpl.service;
//
//
//import lombok.RequiredArgsConstructor;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//@Service
//@RequiredArgsConstructor // Lombok annotation to generate constructor with required dependencies
//public class KafkaConsumerManager {
//
//    @Autowired
//    private final LoggingKafkaConsumer loggingConsumer;
//
//    @Autowired
//    private final KafkaConsumer<String, String> kafkaConsumer;
//
//    // List to keep track of active consumers
//    private final List<AbstractKafkaConsumer> consumers = new ArrayList<>();
//
//    // List to store consumer threads for proper lifecycle management
//    private final List<Thread> consumerThreads = new ArrayList<>();
//
//    /**
//     * Starts Kafka consumers and assigns them to specific partitions.
//     */
//    public void startConsumers() {
//        Map<Integer, Integer> partitionLimits = Map.of(0, 8, 1, 3, 2, 5); // Define partition limits
//        String topic = "Users";
//
//        consumers.add(loggingConsumer); // Adding logging consumer to consumer list
//
//        Thread consumerThread = new Thread(loggingConsumer); // Creating a new thread for the consumer
//        consumerThreads.add(consumerThread); // Storing thread reference
//        consumerThread.start(); // Starting the consumer thread
//    }
//
//    /**
//     * Stops all active consumers and waits for threads to finish execution.
//     */
//    public void stopConsumers() {
//        consumers.forEach(AbstractKafkaConsumer::stop); // Stopping each consumer
//        consumerThreads.forEach(thread -> {
//            try {
//                thread.join(); // Ensuring graceful shutdown of threads
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt(); // Restoring the interrupted status
//            }
//        });
//    }
//}
//


