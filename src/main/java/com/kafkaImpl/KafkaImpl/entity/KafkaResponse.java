package com.kafkaImpl.KafkaImpl.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class KafkaResponse {
    private String userId;
    private int partition;
    private List<String> messages;
}
