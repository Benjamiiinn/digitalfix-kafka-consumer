package com.digitalfix.kafka_consumer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.digitalfix.kafka_consumer.dto.WorkOrderAuditEvent;
import com.digitalfix.kafka_consumer.dto.WorkOrderKafkaEvent;

@Service
public class KafkaProcessingService {
    private static final Logger log =
            LoggerFactory.getLogger(KafkaProcessingService.class);

    public void processWorkOrderEvent(WorkOrderKafkaEvent event) {

        if (event.eventId() == null || event.orderId() == null
                || event.tipo() == null) {
            throw new IllegalArgumentException(
                    "Evento de orden incompleto"
            );
        }

        log.info("Procesando evento Kafka:");
        log.info("Event ID: {}", event.eventId());
        log.info("Orden ID: {}", event.orderId());
        log.info("Tipo: {}", event.tipo());
        log.info("Estado: {}", event.estado());

        switch (event.tipo()) {
            case "ORDEN_CREADA" ->
                    log.info("Nueva orden registrada: {}", event.orderId());

            case "ORDEN_ASIGNADA" ->
                    log.info("Orden asignada: {}", event.orderId());

            case "ORDEN_CERRADA" ->
                    log.info("Orden cerrada: {}", event.orderId());

            default ->
                    log.info("Otro evento de orden: {}", event.tipo());
        }
    }

    public void processAuditEvent(WorkOrderAuditEvent audit) {

        if (audit.eventId() == null || audit.orderId() == null
                || audit.accion() == null) {
            throw new IllegalArgumentException(
                    "Evento de auditoría incompleto"
            );
        }

        log.info("Procesando auditoría Kafka:");
        log.info("Event ID: {}", audit.eventId());
        log.info("Orden ID: {}", audit.orderId());
        log.info("Acción: {}", audit.accion());
        log.info("Estado anterior: {}", audit.estadoAnterior());
        log.info("Estado nuevo: {}", audit.estadoNuevo());
        log.info("Usuario: {}", audit.usuario());
    }
}
