package com.producer.movieproducer.config;

import com.producer.movieproducer.model.Movie;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class MovieProducerConfig {
    @Value("${bootstrap-servers}")
    String bootStrap_server;


    @Bean
    public ProducerFactory<String, Movie> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrap_server);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "movie-producer-tx");
        return new DefaultKafkaProducerFactory<>(configProps, new StringSerializer(), new JacksonJsonSerializer<>());

    }

    @Bean
    public KafkaTemplate<String, Movie> kafkaTemplate() {
        KafkaTemplate<String, Movie> template = new KafkaTemplate<>(producerFactory());
        template.setTransactionIdPrefix("movie-tx-");
        return template;

    }
    @Bean
    NewTopic moviesTopic(){
        return TopicBuilder.name("movies").build();
    }
}
