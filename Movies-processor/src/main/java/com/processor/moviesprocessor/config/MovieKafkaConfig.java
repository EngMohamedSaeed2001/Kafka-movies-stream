package com.processor.moviesprocessor.config;

import com.processor.moviesprocessor.email.EmailDetails;
import com.processor.moviesprocessor.email.EmailServiceImpl;
import com.processor.moviesprocessor.model.Movie;


import com.processor.moviesprocessor.processor.MovieProcessor;
import com.processor.moviesprocessor.serdes.MovieSerde;
import com.processor.moviesprocessor.serdes.SerdesFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.util.*;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
@EnableKafkaStreams

public class MovieKafkaConfig {

    @Autowired
    MovieProcessor movieProcessor;

    @Autowired
    MovieSerde movieSerde;



    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kafkaStreamsConfiguration() {

        Map<String,Object> props = Map.of(
                APPLICATION_ID_CONFIG,"movies-processor-v26",
                BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName(),
                DEFAULT_VALUE_SERDE_CLASS_CONFIG, JacksonJsonSerde.class.getName(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest",
                JacksonJsonDeserializer.TRUSTED_PACKAGES,"com.producer.movieproducer.model.Movie",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE,"com.processor.moviesprocessor.model.Movie",
                JacksonJsonDeserializer.TYPE_MAPPINGS,"com.producer.movieproducer.model.Movie:com.processor.moviesprocessor.model.Movie",
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class
        );
        return new KafkaStreamsConfiguration(props);
    }



    @Bean
    KStream<String,Movie> topologyBuilder(StreamsBuilder streamsBuilder){
        //Serde<ArrayList<Movie>> movieSerdes = SerdesFactory.getSerdesUsingGenerics();

        streamsBuilder.addStateStore(batchStore());
        streamsBuilder.addStateStore(retryStore());

        JacksonJsonSerde<Movie> movieSerdes = movieSerde.movieSerde();


        KStream<String,Movie> inStream = streamsBuilder.stream("movies", Consumed.with(Serdes.String(),movieSerdes));
        //inStream.peek((k,v)-> System.out.println("batch ->"+v.size()));

        KStream<String,Movie> outStream = movieProcessor.movieProcessor(inStream);


        return outStream;
    }



    @Bean
    public StoreBuilder<KeyValueStore<String, ArrayList<Movie>>> batchStore(){

        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("batch-store"),
                Serdes.String(),
                SerdesFactory.getSerdesUsingGenerics()
        );
    }

    @Bean
    public StoreBuilder<KeyValueStore<String, Integer>> retryStore(){
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("retry-store"),
                Serdes.String(),
                Serdes.Integer()
        );
    }







}
