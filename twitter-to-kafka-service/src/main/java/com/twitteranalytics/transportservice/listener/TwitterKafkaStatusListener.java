package com.twitteranalytics.transportservice.listener;

import com.twitteranalytics.config.KafkaConfigData;
import com.twitteranalytics.kafka.avro.model.TwitterAvroModel;
import com.twitteranalytics.kafka.producer.service.KafkaProducer;
import com.twitteranalytics.transportservice.transformer.TwitterStatustoAvroTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {

    private static final Logger log= LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    private final KafkaConfigData kafkaConfigData;
    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;
    private final TwitterStatustoAvroTransformer twitterStatustoAvroTransformer;

    public TwitterKafkaStatusListener(KafkaConfigData kafkaConfigData,
                                      KafkaProducer<Long, TwitterAvroModel> kafkaProducer,
                                      TwitterStatustoAvroTransformer twitterStatustoAvroTransformer) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducer = kafkaProducer;
        this.twitterStatustoAvroTransformer = twitterStatustoAvroTransformer;
    }

    @Override
    public void onStatus(Status status) {
        log.info("Twitter Status with Text {} , sending to Kafka topic {}",
                status.getText(),kafkaConfigData.getTopicName());
        TwitterAvroModel twitterAvroModel=twitterStatustoAvroTransformer.getTwitterAvroModelFromStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(),twitterAvroModel.getUserId(),twitterAvroModel);
    }
}
