package com.processor.moviesprocessor.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Serializer;


public class JsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper
            = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            System.out.println("JsonProcessingException : "+ e.getMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            System.out.println("Exception : "+ e.getMessage());
            throw new RuntimeException(e);
        }

    }
}
