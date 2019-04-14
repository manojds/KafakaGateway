package com.cgd.gwskeleton.consumer;

import com.cgd.gwskeleton.config.GatewayConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class DataConsumerImpl implements DataConsumer {

    private final GatewayConfigs gatewayConfigs;

    String                  topicName;
    int                     partition       = 0;

    Consumer<Long, String>  consumer;

    Logger                  logger          = LoggerFactory.getLogger(DataConsumer.class);

    public DataConsumerImpl(GatewayConfigs gatewayConfigs){
        this.gatewayConfigs = gatewayConfigs;
        topicName       = gatewayConfigs.consume_topics;
    }

    @Override
    public void start() {

    }

    private void subscribeToTopics() {

        logger.info("subscribing to Topics....");

        Properties props = gatewayConfigs.getConsumerProperties();

        consumer = new KafkaConsumer<>(props);

        String[] topics = topicName.split(",");
        List<TopicPartition> topicPartitions = new ArrayList<>();

        for (String topic : topics) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            topicPartitions.add(topicPartition);
        }

        consumer.assign(topicPartitions);
    }

    private void pollRecords(){

        try {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(5));


        } catch (WakeupException e) {
            // ignore for shutdown 2
        } finally {
            consumer.close();
            System.out.println("Closed consumer and we are done");
        }

    }
}
