package com.producer.movieproducer.controller;

import com.producer.movieproducer.service.MovieProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/movie")
public class MovieProducerController {
    @Autowired
    private MovieProducerService movieProducerService;

    @GetMapping("/")
    public ResponseEntity<?>getMovies(){
        movieProducerService.getMovies();
        return ResponseEntity.ok("Movies received successfully");
    }
}
