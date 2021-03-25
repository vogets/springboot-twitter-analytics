package com.twitteranalytics.transportservice.transformer;

import com.twitteranalytics.kafka.avro.model.TwitterAvroModel;
import org.springframework.stereotype.Component;
import twitter4j.Status;

@Component
public class TwitterStatustoAvroTransformer {

    public TwitterAvroModel getTwitterAvroModelFromStatus(Status status)
    {
        return TwitterAvroModel.newBuilder()
                .setId(status.getId())
                .setUserId(status.getUser().getId())
                .setText(status.getText())
                .setCreatedAt(status.getCreatedAt().getTime())
                .build();
    }
}
