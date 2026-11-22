package com.digitalfix.kafka_consumer.dto;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record WorkOrderKafkaEvent(
    String eventId,
        Long orderId,
        String tipo,
        OffsetDateTime fecha,
        String clienteEmail,
        String servicio,
        String descripcion,
        String estado,
        String tecnicoAsignado,
        Long repuestoId
) {}
