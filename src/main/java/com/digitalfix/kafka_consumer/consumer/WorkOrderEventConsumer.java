package com.digitalfix.kafka_consumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.digitalfix.kafka_consumer.dto.WorkOrderKafkaEvent;
import com.digitalfix.kafka_consumer.service.KafkaProcessingService;

import tools.jackson.databind.ObjectMapper;

@Component
public class WorkOrderEventConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(WorkOrderEventConsumer.class);

    private final KafkaProcessingService processingService;
    private final ObjectMapper objectMapper;

    public WorkOrderEventConsumer(
            KafkaProcessingService processingService,
            ObjectMapper objectMapper) {
        this.processingService = processingService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "digitalfix-eventos",
            groupId = "digitalfix-eventos-group"
    )
    public void consume(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) throws Exception {

        log.info(
                "Mensaje recibido: topic={}, partition={}, offset={}",
                record.topic(),
                record.partition(),
                record.offset()
        );

        WorkOrderKafkaEvent event =
                objectMapper.readValue(
                        record.value(),
                        WorkOrderKafkaEvent.class
                );

        processingService.processWorkOrderEvent(event);

        acknowledgment.acknowledge();

        log.info(
                "ACK confirmado para orden {}",
                event.orderId()
        );
    }
}
