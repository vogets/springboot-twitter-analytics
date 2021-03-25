package com.twitteranalytics.transportservice.init.impl;

import com.twitteranalytics.config.KafkaConfigData;
import com.twitteranalytics.kafka.admin.client.KafkaAdminClient;
import com.twitteranalytics.transportservice.init.StreamInitializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamInitializer implements StreamInitializer {

    private static final Logger log= LoggerFactory.getLogger(KafkaStreamInitializer.class);

    private final KafkaConfigData kafkaConfigData;
    private final KafkaAdminClient kafkaAdminClient;

    public KafkaStreamInitializer(KafkaConfigData kafkaConfigData,
                                  KafkaAdminClient kafkaAdminClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaAdminClient = kafkaAdminClient;
    }

    @Override
    public void init() {
       kafkaAdminClient.createTopics();
       kafkaAdminClient.checkSchemaRegistry();
       log.info("Topics with names {} is ready for operations",kafkaConfigData.getTopicNamesToCreate().toArray());

    }
}
