package com.processor.moviesprocessor.processor;

import com.processor.moviesprocessor.model.Movie;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;

public class BatchProcessor implements FixedKeyProcessor<String, Movie, ArrayList<Movie>> {

    private KeyValueStore<String, ArrayList<Movie>> store,retryStore;
    private final int batchSize;
    private FixedKeyProcessorContext<String, ArrayList<Movie>> context;

    public BatchProcessor(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, ArrayList<Movie>> context) {
        this.context = context;
        this.store = context.getStateStore("batch-store");
    }

    @Override
    public void process(FixedKeyRecord<String, Movie> record) {
        String key = record.key();
        Movie value = record.value();

        ArrayList<Movie> list = store.get(key);

        if (list == null) {
            list = new ArrayList<>();
        }

        list.add(value);

        if (list.size() >= batchSize) {
            store.put(key, new ArrayList<>());
            context.forward(record.withValue(list));  // emit batch
        } else {
            store.put(key, list);
            // don't emit
        }
    }

    @Override
    public void close() {
        // cleanup if needed
    }
}