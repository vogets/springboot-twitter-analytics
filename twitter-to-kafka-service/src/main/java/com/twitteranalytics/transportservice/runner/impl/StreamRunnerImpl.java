package com.twitteranalytics.transportservice.runner.impl;



import com.twitteranalytics.config.TwittertoKafkaApplicationConfig;
import com.twitteranalytics.transportservice.listener.TwitterKafkaStatusListener;
import com.twitteranalytics.transportservice.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;


@Component
@ConditionalOnProperty(name="twitter-to-kafka-service.enable-mock-tweets",havingValue = "false",matchIfMissing = true)
public class StreamRunnerImpl  implements StreamRunner {
    private static final Logger log= LoggerFactory.getLogger(StreamRunnerImpl.class);
    private final TwittertoKafkaApplicationConfig twittertoKafkaApplicationConfig;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private TwitterStream twitterStream;

    public StreamRunnerImpl(TwittertoKafkaApplicationConfig twittertoKafkaApplicationConfig,
                            TwitterKafkaStatusListener twitterKafkaStatusListener)
    {
        this.twittertoKafkaApplicationConfig=twittertoKafkaApplicationConfig;
        this.twitterKafkaStatusListener=twitterKafkaStatusListener;
    }
    @Override
    public void start() throws TwitterException {
        twitterStream= TwitterStreamFactory.getSingleton();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();


    }

    @PreDestroy
    public void shutdown()
    {
        if(twitterStream!=null){
            log.info("TwitterStream is not null, closing it");
            twitterStream.shutdown();
        }
    }
    private void addFilter()
    {
        String[] keywords=twittertoKafkaApplicationConfig.getTwitterkeywords().toArray(new String[0]);
        FilterQuery filterQuery=new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        log.info("Started filtering twitter Stream for keywords {}", Arrays.toString(keywords));
    }
}
