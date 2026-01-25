package com.processor.moviesprocessor.config;

import com.processor.moviesprocessor.model.Movie;


import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaStreamBrancher;

import java.util.function.Function;

@Configuration
public class MovieProcessorConfig {

    @Bean
    Function<KStream<String, Movie>,KStream<String,Movie>>movieProcessor(){
        return inputStream-> new KafkaStreamBrancher<String,Movie>()
                .branch((key,movie)-> movie.getGenres().get(0).equalsIgnoreCase("drama"),kstream->kstream.to("drama.movies"))
                .branch((key,movie)-> movie.getGenres().get(0).equalsIgnoreCase("documentary"),kstream->kstream.to("documentary.movies"))
                .branch((key,movie)-> movie.getGenres().get(0).equalsIgnoreCase("crime"),kstream->kstream.to("crime.movies"))
                .branch((key,movie)-> movie.getGenres().get(0).equalsIgnoreCase("action"),kstream->kstream.to("action.movies"))
                .branch((key,movie)-> movie.getGenres().get(0).equalsIgnoreCase("comedy"),kstream->kstream.to("comedy.movies"))
                .branch((key,movie)-> movie.isAdult(),kstream->kstream.to("adult.movies"))
                .defaultBranch(kstream->kstream.to("others"))
                .onTopOf(inputStream);

    }
}
