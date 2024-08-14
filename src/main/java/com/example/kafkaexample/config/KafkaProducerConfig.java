/*package com.example.kafkaexample.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;

@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.producer.key-serializer}")
    private String keySerializer;

    @Value("${kafka.producer.value-serializer}")
    private String valueSerializer;

    @Bean
    public Map<String, Object> producerConfigs() throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Class.forName(keySerializer));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Class.forName(valueSerializer));
        return props;
    }
}
*/
