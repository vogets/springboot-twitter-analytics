package com.twitteranalytics.kafka.producer.service.impl;

import com.twitteranalytics.kafka.avro.model.TwitterAvroModel;
import com.twitteranalytics.kafka.producer.service.KafkaProducer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PreDestroy;


@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger log= LoggerFactory.getLogger(TwitterKafkaProducer.class);
    private KafkaTemplate<Long,TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long,TwitterAvroModel> template)
    {
        this.kafkaTemplate=template;
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        log.info("Sending message ={} to the topic = {}",message,topicName);
        ListenableFuture<SendResult<Long,TwitterAvroModel>> kafkaResultFuture=kafkaTemplate.send(topicName,key,message);
        addCallback(topicName, kafkaResultFuture);
    }

    @PreDestroy
    public void close()
    {
        if(kafkaTemplate!=null){
            log.info("Closing kafka Template");
            kafkaTemplate.destroy();
        }
    }

    private void addCallback(String topicName, ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture) {
        kafkaResultFuture.addCallback(new ListenableFutureCallback<SendResult<Long, TwitterAvroModel>>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.error("Error occured while sending message to the topic {},error {}", topicName,throwable.getMessage());

            }

            @Override
            public void onSuccess(SendResult<Long, TwitterAvroModel> longTwitterAvroModelSendResult) {
                RecordMetadata recordMetadata=longTwitterAvroModelSendResult.getRecordMetadata();
                log.debug("Received new Metadata, Topic: {}; Partition: {}; Offset {} ; TimeStamp {} , at time {}",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        recordMetadata.timestamp(),
                        System.nanoTime());

            }
        });
    }
}
