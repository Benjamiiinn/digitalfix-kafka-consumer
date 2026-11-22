package com.digitalfix.kafka_consumer.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration 
public class KafkaErrorConfig {

    @Bean
    public KafkaTemplate<String, String> dltKafkaTemplate(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {

        Map<String, Object> props = new HashMap<>();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class
        );

        props.put(
                ProducerConfig.ACKS_CONFIG,
                "all"
        );

        props.put(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true
        );

        ProducerFactory<String, String> producerFactory =
                new DefaultKafkaProducerFactory<>(props);

        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> dltKafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        dltKafkaTemplate
                );

        // Dos reintentos adicionales, separados por 2 segundos.
        FixedBackOff backOff = new FixedBackOff(2000L, 2L);

        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, backOff);

        // Si falla la publicación en DLT, no marcar como recuperado.
        errorHandler.setResetStateOnRecoveryFailure(true);

        return errorHandler;
    }
}
