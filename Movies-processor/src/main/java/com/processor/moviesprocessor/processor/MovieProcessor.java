package com.processor.moviesprocessor.processor;

import com.processor.moviesprocessor.model.Movie;
import com.processor.moviesprocessor.serdes.MovieSerde;
import com.processor.moviesprocessor.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import org.apache.kafka.streams.kstream.Suppressed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
@EnableScheduling
public class MovieProcessor {
    @Autowired
    KafkaTemplate<String,Movie> kafkaTemplate;

    @Autowired
    MovieSerde movieSerde;

    int batchCount=0;
    AtomicLong startTime=new AtomicLong(0);
    AtomicBoolean rollback=new AtomicBoolean(false);

    public KStream<String, Movie> movieProcessor(KStream<String,Movie> inputStream) {

        KStream<String, ArrayList<Movie>> batch = createBatch(inputStream,10);

        //System.out.println(batch);

            new KafkaStreamBrancher<String, ArrayList<Movie>>()
                    .branch((key, movie) -> hasGenre(movie,"drama"), kstream -> sendBatch(kstream,"drama.movies"))
                    .branch((key, movie) -> hasGenre(movie,"documentary"), kstream -> sendBatch(kstream,"documentary.movies"))
                    .branch((key, movie) -> hasGenre(movie,"crime"), kstream -> sendBatch(kstream,"crime.movies"))
                    .branch((key, movie) -> hasGenre(movie,"action"), kstream -> sendBatch(kstream,"action.movies"))
                    .branch((key, movie) -> hasGenre(movie,"comedy"), kstream -> sendBatch(kstream,"comedy.movies"))
                    //.branch((key, movie) -> movie.isAdult(), kstream -> kstream.to("adult.movies"))
                    .defaultBranch(kstream -> sendBatch(kstream,"others"))
                    .onTopOf(batch);

            return inputStream;
        };


    private KStream<String, ArrayList<Movie>> createBatch(
            KStream<String, Movie> inputStream , int batchSize) {




        Serde<ArrayList<Movie>> batchSerde = SerdesFactory.getSerdesUsingGenerics();
        ;

        KStream<String, ArrayList<Movie>> aggregatedStream = inputStream
                .peek((k,v)->{
                    if(startTime.get()==0){
                        startTime.set(System.currentTimeMillis());
                        System.out.println("Stream started at: "+startTime.get());
                    }
                })
                .selectKey((k,v) ->
                        v.getGenres().getFirst().toLowerCase()
                )
                .groupByKey()
                .aggregate(
                        ArrayList::new,
                        (key, movie, list) -> {
                            list.add(movie);
                            return list;
                        },
                        Materialized.with(Serdes.String(), batchSerde)
                )
                .toStream();

        KStream<String,ArrayList<Movie>>finalStream =aggregatedStream
                .peek((k,v)->{
                   batchCount++;
                    System.out.println("Key = "+k+"V = "+v+"\n");

                });

        long endTime = System.currentTimeMillis();
        System.out.println("Stream ended at: " + endTime);

        System.out.println("Stream takes : " + (endTime - startTime.get()) + "mils");

        return finalStream;
    }

    private boolean hasGenre(ArrayList<Movie> batch, String genre) {

        if (batch.isEmpty()) return false;

        Movie m = batch.getFirst();

        return m.getGenres() != null
                && !m.getGenres().isEmpty()
                && m.getGenres().getFirst()
                .equalsIgnoreCase(genre);
    }

    private void sendBatch(
            KStream<String, ArrayList<Movie>> stream,
            String topic
    ) {
        stream.foreach((key, batch) -> {

            System.out.println(
                    "Sending batch to " + topic +
                            " size=" + batch.size()
            );

            batch.forEach(movie ->
                    kafkaTemplate.send(topic, movie.getId(), movie)
            );
        });
    }

    @Scheduled(fixedRate = 30000)
    void IsStreamFinished(){
        long currentTime = System.currentTimeMillis();
        System.out.println("batch count = "+batchCount);
        if(Math.abs(currentTime-startTime.get())>=20000 && batchCount<10){
            rollback.set(true);
        }
    }

}
