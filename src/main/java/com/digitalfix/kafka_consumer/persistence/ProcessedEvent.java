package com.digitalfix.kafka_consumer.persistence;

import java.time.OffsetDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Registro persistente de eventos ya procesados.
 *
 * La clave primaria es el eventId, por lo que Kafka puede reentregar un
 * mensaje sin que el consumidor ejecute dos veces el mismo efecto.
 */
@Entity
@Table(name = "processed_event")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id", length = 64, nullable = false)
    private String eventId;

    @Column(name = "topic", length = 128, nullable = false)
    private String topic;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "event_type", length = 64, nullable = false)
    private String eventType;

    @Column(name = "processed_at", nullable = false)
    private OffsetDateTime processedAt;

    protected ProcessedEvent() {
        // JPA
    }

    public ProcessedEvent(
            String eventId,
            String topic,
            Long orderId,
            String eventType,
            OffsetDateTime processedAt) {
        this.eventId = eventId;
        this.topic = topic;
        this.orderId = orderId;
        this.eventType = eventType;
        this.processedAt = processedAt;
    }

    public String getEventId() {
        return eventId;
    }

    public String getTopic() {
        return topic;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public OffsetDateTime getProcessedAt() {
        return processedAt;
    }
}
