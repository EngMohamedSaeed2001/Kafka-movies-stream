package com.processor.moviesprocessor.serdes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.processor.moviesprocessor.model.Movie;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;

public class SerdesFactory {

    static public Serde<ArrayList<Movie>> getSerdesUsingGenerics() {
        JsonSerializer<ArrayList<Movie>> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<ArrayList<Movie>> jsonDeserializer = new JsonDeserializer<>(new TypeReference<ArrayList<Movie>>() {});

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
