package com.twitteranalytics.kafka.admin.config;

import com.twitteranalytics.config.KafkaConfigData;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

import java.util.HashMap;
import java.util.Map;

@EnableRetry
@Configuration
public class KafkaAdminConfig {
    private final KafkaConfigData kafkaConfigData;

    public KafkaAdminConfig(KafkaConfigData kafkaConfigData)
    {
        this.kafkaConfigData=kafkaConfigData;
    }

    @Bean
    public AdminClient adminClient()
    {
        Map configMap=new HashMap();
        configMap.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,kafkaConfigData.getBootStrapServers());
        return AdminClient.create(configMap);
    }
}
