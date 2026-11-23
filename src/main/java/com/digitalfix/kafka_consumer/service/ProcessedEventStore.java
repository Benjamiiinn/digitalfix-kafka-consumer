package com.digitalfix.kafka_consumer.service;

import java.time.OffsetDateTime;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.digitalfix.kafka_consumer.persistence.ProcessedEvent;
import com.digitalfix.kafka_consumer.persistence.ProcessedEventRepository;

@Service
public class ProcessedEventStore {

    private final ProcessedEventRepository repository;

    public ProcessedEventStore(ProcessedEventRepository repository) {
        this.repository = repository;
    }

    public boolean isProcessed(String eventId) {
        return repository.existsById(eventId);
    }

    @Transactional
    public void markProcessed(
            String eventId,
            String topic,
            Long orderId,
            String eventType) {

        repository.save(new ProcessedEvent(
                eventId,
                topic,
                orderId,
                eventType,
                OffsetDateTime.now()
        ));
    }

    public List<ProcessedEvent> recent() {
        return repository.findTop50ByOrderByProcessedAtDesc();
    }
}
