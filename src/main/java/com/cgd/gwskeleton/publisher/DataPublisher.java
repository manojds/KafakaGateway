package com.cgd.gwskeleton.publisher;

public interface DataPublisher {

    void start();

    Long getLastPublishedSequenceNumber();

    boolean publishMessage(Long sequenceNumber, Long sequenceNumberOffset, String Message);
}
