package com.consumer.moviesconsumer.config;

import com.consumer.moviesconsumer.model.Movie;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class MovieConsumerConfig {
    Logger logger = LoggerFactory.getLogger(MovieConsumerConfig.class);

    @Bean
    public Consumer<KStream<String, Movie>>otherConsumer(){
        return inputStream->inputStream

                .groupBy((key,movie)->movie.getGenres().get(0))
                .count(Materialized.as("No.others-movies"))
                .toStream()
                .peek((genre,count)->{
                    logger.info("{} genre has {} movies",genre,count);
                });
    }

    @Bean
    public Consumer<KStream<String, Movie>>comedyConsumer(){
        return inputStream->inputStream
                .groupBy((key,movie)->movie.getGenres().get(0))
                .count(Materialized.as("No.comedy-movies"))
                .toStream()
                .peek((genre,count)->{
                    logger.info("{} genre has {} movies",genre,count);
                });
    }
    @Bean
    public Consumer<KStream<String, Movie>>dramaConsumer(){
        return inputStream->inputStream

                .groupBy((key,movie)->movie.getGenres().get(0))
                .count(Materialized.as("No.drama-movies"))
                .toStream()
                .peek((genre,count)->{
                    logger.info("{} genre has {} movies",genre,count);
                });
    }
    @Bean
    public Consumer<KStream<String, Movie>>actionConsumer(){
        return inputStream->inputStream

                .groupBy((key,movie)->movie.getGenres().get(0))
                .count(Materialized.as("No.action-movies"))
                .toStream()
                .peek((genre,count)->{
                    logger.info("{} genre has {} movies",genre,count);
                });
    }
    @Bean
    public Consumer<KStream<String, Movie>>docConsumer(){
        return inputStream->inputStream

                .groupBy((key,movie)->movie.getGenres().get(0))
                .count(Materialized.as("No.documentary-movies"))
                .toStream()
                .peek((genre,count)->{
                    logger.info("{} genre has {} movies",genre,count);
                });
    }

    @Bean
    public Consumer<KStream<String, Movie>>adultConsumer(){
        return inputStream->inputStream

                .groupBy((key,movie)->movie.getGenres().get(0))
                .count(Materialized.as("No.adult-movies"))
                .toStream()
                .peek((genre,count)->{
                    logger.info("{} genre has {} movies",genre,count);
                });
    }

    @Bean
    public Consumer<KStream<String, Movie>>crimeConsumer(){
        return inputStream->inputStream
                .groupBy((key,movie)->movie.getGenres().get(0))
                .count(Materialized.as("No.crime-movies"))
                .toStream()
                .peek((genre,count)->{
                    logger.info("{} genre has {} movies",genre,count);
                });
    }


}
