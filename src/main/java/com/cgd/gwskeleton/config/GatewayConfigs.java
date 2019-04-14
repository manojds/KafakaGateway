package com.cgd.gwskeleton.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;

import java.util.Properties;

public class GatewayConfigs {

    @Value("${kafka.brokers:localhost:9092}")
    public String KAFKA_BROKERS ;

    @Value("${kafka.message_count:1000}")
    public Integer MESSAGE_COUNT;

    @Value("${kafka.client_id:client1}")
    public String CLIENT_ID;

    @Value("${kafka.publish_topic:demo}")
    public String publish_topic;

    @Value("${kafka.consume_topics:demo,ccg,monitor}")
    public String consume_topics;

    @Value("${kafka.group_id:consumerGroup1}")
    public String GROUP_ID_CONFIG;

    @Value("${kafka.max_no_message_coount_found:100}")
    public Integer MAX_NO_MESSAGE_FOUND_COUNT;

    @Value("${kafka.offset_rest_latest:latest}")
    public String OFFSET_RESET_LATEST;

    @Value("${kafka.offset_rest_earlier:earliest}")
    public String OFFSET_RESET_EARLIER="earliest";

    @Value("${kafka.max_poll_records:1}")
    public Integer MAX_POLL_RECORDS=1;


    public Properties getPublisherProperties() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());

        return props;
    }

    public Properties getConsumerProperties(){
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER);

        return props;
    }
}
