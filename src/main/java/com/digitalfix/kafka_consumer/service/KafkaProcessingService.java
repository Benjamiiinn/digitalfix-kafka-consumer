package com.digitalfix.kafka_consumer.service;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.digitalfix.kafka_consumer.dto.WorkOrderAuditEvent;
import com.digitalfix.kafka_consumer.dto.WorkOrderKafkaEvent;

@Service
public class KafkaProcessingService {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaProcessingService.class);

    private static final Set<String> TIPOS_NEGOCIO = Set.of(
            "ORDEN_CREADA",
            "ORDEN_ASIGNADA",
            "ORDEN_EN_DESPLAZAMIENTO",
            "ORDEN_EN_EJECUCION",
            "ORDEN_CERRADA",
            "ORDEN_CANCELADA"
    );

    public void processWorkOrderEvent(WorkOrderKafkaEvent event) {

        require(event != null, "Evento de orden nulo");
        require(notBlank(event.eventId()), "eventId requerido");
        require(event.orderId() != null, "orderId requerido");
        require(notBlank(event.tipo()), "tipo requerido");
        require(
                TIPOS_NEGOCIO.contains(event.tipo()),
                "tipo no soportado: " + event.tipo()
        );
        require(event.fecha() != null, "fecha requerida");

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

            case "ORDEN_EN_DESPLAZAMIENTO" ->
                    log.info("Tecnico en desplazamiento: {}", event.orderId());

            case "ORDEN_EN_EJECUCION" ->
                    log.info("Orden en ejecucion: {}", event.orderId());

            case "ORDEN_CERRADA" ->
                    log.info("Orden cerrada: {}", event.orderId());

            case "ORDEN_CANCELADA" ->
                    log.info("Orden cancelada: {}", event.orderId());

            default ->
                    log.warn("Tipo de evento no reconocido: {}", event.tipo());
        }
    }

    public void processAuditEvent(WorkOrderAuditEvent audit) {

        require(audit != null, "Evento de auditoria nulo");
        require(notBlank(audit.eventId()), "eventId requerido");
        require(audit.orderId() != null, "orderId requerido");
        require(notBlank(audit.accion()), "accion requerida");
        require(notBlank(audit.estadoNuevo()), "estadoNuevo requerido");
        require(notBlank(audit.usuario()), "usuario requerido");
        require(audit.fecha() != null, "fecha requerida");

        log.info("Procesando auditoria Kafka:");
        log.info("Event ID: {}", audit.eventId());
        log.info("Orden ID: {}", audit.orderId());
        log.info("Accion: {}", audit.accion());
        log.info("Estado anterior: {}", audit.estadoAnterior());
        log.info("Estado nuevo: {}", audit.estadoNuevo());
        log.info("Usuario: {}", audit.usuario());
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }
}
