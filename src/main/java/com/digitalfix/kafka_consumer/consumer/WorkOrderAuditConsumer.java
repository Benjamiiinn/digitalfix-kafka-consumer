package com.digitalfix.kafka_consumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.digitalfix.kafka_consumer.dto.WorkOrderAuditEvent;
import com.digitalfix.kafka_consumer.service.KafkaProcessingService;
import com.digitalfix.kafka_consumer.service.ProcessedEventStore;

import tools.jackson.databind.ObjectMapper;

@Component 
public class WorkOrderAuditConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(WorkOrderAuditConsumer.class);

    private final KafkaProcessingService processingService;
    private final ProcessedEventStore processedEventStore;
    private final ObjectMapper objectMapper;

    public WorkOrderAuditConsumer(
            KafkaProcessingService processingService,
            ProcessedEventStore processedEventStore,
            ObjectMapper objectMapper) {
        this.processingService = processingService;
        this.processedEventStore = processedEventStore;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "digitalfix-auditoria",
            groupId = "digitalfix-auditoria-group"
    )
    public void consume(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) throws Exception {

        log.info(
                "Auditoría recibida: topic={}, partition={}, offset={}",
                record.topic(),
                record.partition(),
                record.offset()
        );

        WorkOrderAuditEvent audit =
                objectMapper.readValue(
                        record.value(),
                        WorkOrderAuditEvent.class
                );

        if (audit.eventId() == null || audit.eventId().isBlank()) {
            throw new IllegalArgumentException(
                    "Evento de auditoría sin eventId"
            );
        }

        if (processedEventStore.isProcessed(audit.eventId())) {
            log.info(
                    "Auditoría duplicada ignorada: eventId={}",
                    audit.eventId()
            );
            acknowledgment.acknowledge();
            return;
        }

        processingService.processAuditEvent(audit);

        processedEventStore.markProcessed(
                audit.eventId(),
                record.topic(),
                audit.orderId(),
                audit.accion()
        );

        acknowledgment.acknowledge();

        log.info(
                "ACK confirmado para auditoría {}",
                audit.eventId()
        );
    }
}
