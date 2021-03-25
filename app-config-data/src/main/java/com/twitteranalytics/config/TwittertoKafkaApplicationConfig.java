package com.twitteranalytics.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@Configuration
@Component
@ConfigurationProperties("twitter-to-kafka-service")
public class TwittertoKafkaApplicationConfig {
    private String welcomemessage;
    private List<String> twitterkeywords;
    private Boolean enableMockTweets;
    private Long mockSleepMs;
    private Integer mockMinTweetLength;
    private Integer mockMaxTweetLength;
}
