package com.processor.moviesprocessor.serdes;

import com.processor.moviesprocessor.model.Movie;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

@Component
public class MovieSerde {
    public ObjectMapper getObjectMapper() {
        return new ObjectMapper();
    }

    public JacksonJsonSerde<Movie> movieSerde() {

        JacksonJsonSerde<Movie> movieSerde = new JacksonJsonSerde<>(Movie.class);
        movieSerde.ignoreTypeHeaders();
        movieSerde.deserializer().addTrustedPackages("*");
        movieSerde.deserializer().setUseTypeHeaders(false);

        return movieSerde;
    }

}
