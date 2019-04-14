package com.cgd.gwskeleton.publisher;

import com.cgd.gwskeleton.config.GatewayConfigs;
import com.cgd.gwskeleton.core.GwManager;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class DataPublisherImpl implements DataPublisher{

    private final GatewayConfigs gatewayConfigs;

    Producer<Long, String>  kProducer;
    Long                    lastPublishedSN;
    String                  topicName       ;
    int                     partition       = 0;

    //ExecutorService         executorService = null;

    Logger                  logger          = LoggerFactory.getLogger(GwManager.class);

    private class ProducerCallback implements Callback {
        final Long key;
        final String message;

        private ProducerCallback(Long key,  String message) {
            this.key = key;
            this.message = message;
        }

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception exc) {

            if (recordMetadata != null){
                logger.debug("Record sent with key ["+ key +"]");
            }
            if (exc != null) {
                logger.error("Failed to publish message. Exception ["+exc.getMessage()+"], Record data: key ["+ key+"], message ["+ message +"]");
            }
        }
    }

    public DataPublisherImpl(GatewayConfigs gatewayConfigs){
        this.gatewayConfigs = gatewayConfigs;
        topicName       = gatewayConfigs.publish_topic;
    }


    @Override
    public void start(){

        initializeLastPublishedSequenceNumber();

        createPublisher();
    }

    @Override
    public Long getLastPublishedSequenceNumber() {
        return lastPublishedSN;
    }

    @Override
    public boolean publishMessage(Long sequenceNumber, Long sequenceNumberOffset, String message) {
        if (sequenceNumber != lastPublishedSN + sequenceNumberOffset){
            logger.warn("Sequence Gap Detected.. LastPublishedSequenceNumber="+lastPublishedSN+", CurrentSequencenumber="+sequenceNumber+"+, Offset Communicated="+sequenceNumberOffset);
            return false;
        }

        sendMessage(sequenceNumber, message);

        lastPublishedSN = sequenceNumber;

        return true;
    }

    public void initializeLastPublishedSequenceNumber() {

        logger.info("initializing Last Published SequenceNumber....");

        Consumer<Long, String> consumer = createConsumer() ;

        TopicPartition topicPartition = new TopicPartition(topicName, partition);
        List<TopicPartition> partitions = Collections.singletonList(topicPartition);

        consumer.assign(partitions);

        try {


            Map<TopicPartition,Long> endOffsets = consumer.endOffsets(partitions);

            Long lastOffset = endOffsets.get(topicPartition);

            if (lastOffset > 0) {
                consumer.seek(topicPartition, lastOffset - 1);

                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(5));

                if (records.isEmpty()) {
                    logger.info("Received zero records from poll for initializing Last Published SequenceNumber");
                }
                for (ConsumerRecord<Long, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                    lastPublishedSN = record.key();
                }

            }else{
                logger.info("Last Offset Available in the Kafka is ["+lastOffset+"], setting Last Published SN to zero.");
                lastPublishedSN = 0L;
            }


        } catch (WakeupException e) {
            // ignore for shutdown 2
        } finally {
            consumer.close();
            System.out.println("Closed consumer and we are done");
        }

        if (lastPublishedSN == null || lastPublishedSN <0 ){
            String errorMessage = "Last Published SN to topic ["+topicName+"] and partition ["+partition+"] is in invalid state. Last Published Sequence Number = "+lastPublishedSN+"";

            logger.info(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        logger.info("Last Published SN to topic ["+topicName+"] and partition ["+partition+"] is ["+lastPublishedSN+"]");
    }



    private void createPublisher() {
        Properties props = gatewayConfigs.getPublisherProperties();

        kProducer = new KafkaProducer<>(props);
    }

    public void sendMessage(String messsage){

        sendMessage(lastPublishedSN +1 , messsage);

    }

    public void sendMessage(Long sequenceNumber, String messsage){

        ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topicName, sequenceNumber, messsage);

        ProducerCallback callback = new ProducerCallback(sequenceNumber, messsage);

        try {
            kProducer.send(record, callback);
        }
        catch (RuntimeException ex){
            callback.onCompletion(null, ex);
        }

    }

    public Consumer<Long, String> createConsumer() {

        Properties props = gatewayConfigs.getConsumerProperties();

        Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        //consumer.subscribe(Collections.singletonList(topicName));

        return consumer;

    }
    
}
