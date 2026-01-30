package com.processor.moviesprocessor.processor;

import com.processor.moviesprocessor.email.EmailDetails;
import com.processor.moviesprocessor.email.EmailServiceImpl;
import com.processor.moviesprocessor.model.Movie;
import com.processor.moviesprocessor.serdes.MovieSerde;
import com.processor.moviesprocessor.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
@EnableScheduling
public class MovieProcessor {
    @Autowired
    KafkaTemplate<String,Movie> kafkaTemplate;

    private KeyValueStore<String, ArrayList<Movie>> retryStore;
    private FixedKeyProcessorContext<String, ArrayList<Movie>> context;

    @Autowired
    MovieSerde movieSerde;
    @Autowired
    EmailServiceImpl emailService;

    List<ArrayList<Movie>> moviesBatchList =
            Collections.synchronizedList(new ArrayList<>());
    AtomicLong batchCount = new AtomicLong(0);

    AtomicLong startTime=new AtomicLong(-1);
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

        KTable<String, ArrayList<Movie>> table =
                inputStream
                        .peek((k,v)->{
                            if(startTime.compareAndSet(-1, System.currentTimeMillis())){
                                System.out.println("Stream started at: "+startTime.get());
                            }
                        })
                        .selectKey((k,v)-> v.getGenres().getFirst().toLowerCase())
                        .processValues(
                                () -> new BatchProcessor(10),
                                "batch-store"
                        ).toTable(
                                Materialized.with(Serdes.String(),batchSerde)
                        );

        KStream<String, ArrayList<Movie>> finalStream = table
                .toStream()
                .filter((k,v) -> v.size() >= batchSize)
                .peek((k,v)->{
                    batchCount.incrementAndGet();
                    System.out.println("Key = "+k+"V = "+v+"\n");

                });


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
        if(rollback.get()){
            doFailure();
            return;
        }

        stream.foreach((key, batch) -> {

            System.out.println(
                    "Sending batch to " + topic +
                            " size=" + batch.size()
            );

             batch.forEach(movie ->
                    kafkaTemplate.send(topic, movie.getId(), movie)
            );

             moviesBatchList.add(batch);

        });
    }

    @Scheduled(fixedRate = 30000)
    void IsStreamFinished(){

        if(startTime.get() == -1){
            return;
        }

        long currentTime = System.currentTimeMillis();
        long elapsed = currentTime - startTime.get();

        System.out.println("batch count = "+batchCount.get());


        if(elapsed >= 600000 && batchCount.get() < 10){
            rollback.set(true);
            doFailure();
            return;
        }


        if(!rollback.get() && batchCount.get() >= 10){
            System.out.println("Start write excel file");
            doSuccess(moviesBatchList);
            rollback.set(true);
        }
    }

    void doFailure(){
        moviesBatchList.clear();
        rollback.set(false);
    }

    void doSuccess(List<ArrayList<Movie>> moviesData){
        String fileName = writeExcel(moviesData);
        sendEmail(fileName);
    }

    String writeExcel(List<ArrayList<Movie>> moviesData ){
        Excel excel = new Excel();
        return excel.writeMultipleBatchesToExcel(moviesData);
    }
    void sendEmail(String attachment){
        String email="abdelrahmn.ahmed119@gmail.com";
        String name =email.split("@")[0];
        EmailDetails emailDetails =new EmailDetails(email,"Dear Mr. "+name +", this is kafka test. thanks <3","TEST-KAFKA",attachment);
        emailService.sendMailWithAttachment(emailDetails);
    }


}
