package com.digitalfix.kafka_consumer.controller;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.digitalfix.kafka_consumer.persistence.ProcessedEvent;
import com.digitalfix.kafka_consumer.service.ProcessedEventStore;

@RestController
@RequestMapping("/api/processed-events")
public class ProcessedEventController {

    private final ProcessedEventStore store;

    public ProcessedEventController(ProcessedEventStore store) {
        this.store = store;
    }

    @GetMapping
    public List<ProcessedEvent> list() {
        return store.recent();
    }
}
