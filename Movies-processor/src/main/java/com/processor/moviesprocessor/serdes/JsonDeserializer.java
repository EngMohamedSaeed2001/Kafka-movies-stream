package com.processor.moviesprocessor.serdes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.processor.moviesprocessor.model.Movie;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.ArrayList;


public class JsonDeserializer<T> implements Deserializer<T> {

    private final TypeReference<ArrayList<Movie>> destinationClass;

    public JsonDeserializer(TypeReference<ArrayList<Movie>> destinationClass) {
        this.destinationClass = destinationClass;
    }



    private final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public T deserialize(String topic, byte[] data) {
        if(data==null){
            return null;
        }
        try {
            return (T) objectMapper.readValue(data, destinationClass);
        } catch (IOException e) {
            System.out.println("IOException in Deserializer: "+ e.getMessage());
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            System.out.println("Exception  in Deserializer : "+ e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
