package com.producer.movieproducer.service;

import com.producer.movieproducer.model.Movie;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


@Service
public class MovieProducerService {
    public int i=0;
    public int j=0;

    @Autowired
    private KafkaTemplate<String, Movie> kafkaTemplate;
    Logger logger = LoggerFactory.getLogger(MovieProducerService.class);
    private final String TOPIC_NAME="movies";
    private String API_KEY = "2d77e0a269mshd89184456267120p1fac01jsn09ae866f1dc5";
    private String BASE_URL="https://imdb236.p.rapidapi.com/api/imdb/top250-movies";

    public void getMovies(){

        Flux<Movie> movieFlux =  WebClient.create()
            .get()
            .uri(BASE_URL)
            .header("x-rapidapi-key", API_KEY)
            .header("x-rapidapi-host", "imdb236.p.rapidapi.com")
            .accept(MediaType.APPLICATION_JSON)
            .retrieve()
            .bodyToFlux(Movie.class);

        sendMovies(movieFlux);



    }

    private void sendMovies(Flux<Movie> movieFlux) {
        movieFlux
                .buffer(10)
                .concatMap(batch ->
                        Mono.fromRunnable(() -> sendBatch(batch))
                        .doOnSuccess(unused ->{
                            System.out.println("Batch of size " + batch.size() + " finished sending");
                            System.out.println("=================================================================");
                        })
                        .delaySubscription(Duration.ofSeconds(1+i))
                )
                .doOnComplete(() -> System.out.println("All batches finished = "+i))
                .subscribe();


    }
    private void sendBatch(List<Movie> batch){
        j+=1;


        kafkaTemplate.executeInTransaction(kafka -> {
            System.out.println("numb = "+ j +" original size = "+batch.size());
            batch.forEach((movie)->{
                kafka.send(TOPIC_NAME,movie.getId() ,movie);
            });

            return null;
        });
        i+=1;

        System.out.println("Batch sent = "+batch.size());

    }


}
