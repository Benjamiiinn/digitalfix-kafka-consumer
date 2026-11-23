package com.digitalfix.kafka_consumer.persistence;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository
        extends JpaRepository<ProcessedEvent, String> {

    List<ProcessedEvent> findTop50ByOrderByProcessedAtDesc();
}
