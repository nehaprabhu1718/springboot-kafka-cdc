package com.example.kafkaexample.kafka.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.producer.key-serializer}")
    private String keySerializer;

    @Value("${kafka.producer.value-serializer}")
    private String valueSerializer;

    @Value("${spring.kafka.properties.sasl.mechanism}")
    private String saslMechanism;

    @Value("${spring.kafka.properties.sasl.jaas.config}")
    private String saslJaasConfig;

    @Value("${spring.kafka.properties.security.protocol}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.session.timeout.ms}")
    private String sessionTimeoutMs;

    @Bean
    public Map<String, Object> producerConfigs() throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Class.forName(keySerializer));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Class.forName(valueSerializer));
        props.put("sasl.mechanism", saslMechanism);
        props.put("sasl.jaas.config", saslJaasConfig);
        props.put("security.protocol", securityProtocol);
        props.put("session.timeout.ms", sessionTimeoutMs);
        return props;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() throws Exception {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() throws Exception {
        return new KafkaTemplate<>(producerFactory());
    }
}
