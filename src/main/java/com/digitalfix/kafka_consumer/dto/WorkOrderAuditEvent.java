package com.digitalfix.kafka_consumer.dto;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record WorkOrderAuditEvent(
        String eventId,
        Long orderId,
        String accion,
        String estadoAnterior,
        String estadoNuevo,
        String usuario,
        OffsetDateTime fecha
) {}
