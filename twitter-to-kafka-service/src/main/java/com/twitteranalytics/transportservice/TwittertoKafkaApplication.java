package com.twitteranalytics.transportservice;


import com.twitteranalytics.config.TwittertoKafkaApplicationConfig;
import com.twitteranalytics.transportservice.init.StreamInitializer;
import com.twitteranalytics.transportservice.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication(scanBasePackages={
        "com.twitteranalytics"})
public class TwittertoKafkaApplication implements CommandLineRunner {

   private static final Logger log=LoggerFactory.getLogger(TwittertoKafkaApplication.class);

   private final StreamInitializer streamInitializer;

   private final StreamRunner streamRunner;

    public TwittertoKafkaApplication(
            StreamRunner streamRunner,StreamInitializer streamInitializer) {
        this.streamInitializer=streamInitializer;
        this.streamRunner=streamRunner;
    }


    public static void main(String[] args) {
        SpringApplication.run(TwittertoKafkaApplication.class, args);
    }


    @Override
    public void run(String... args) throws Exception {
        log.info("App Starts");
        streamInitializer.init();
        streamRunner.start();
    }
}
