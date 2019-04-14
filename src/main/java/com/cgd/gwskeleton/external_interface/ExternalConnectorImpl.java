package com.cgd.gwskeleton.external_interface;


import com.cgd.gwskeleton.publisher.DataPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExternalConnectorImpl implements ExternalConnector{



    Long            nextExpectedSN  = 0L;
    ExecutorService executorService = null;
    BufferedReader  reader          = null;
    DataPublisher   publisher       = null;
    Logger          logger          = LoggerFactory.getLogger(ExternalConnector.class);

    public ExternalConnectorImpl(){
    }

    @Override
    public void start(DataPublisher publisher) {

        this.publisher = publisher;
        nextExpectedSN = publisher.getLastPublishedSequenceNumber() + 1;

        openFile();

        seekToPosition(nextExpectedSN);

        executorService = Executors.newSingleThreadExecutor();

        executorService.execute(this::processNextMessage);
    }

    private void processNextMessage(){

        String record= getNextRecord();

        if (record == null){
            logger.info("End of the file reached.......");
            return;
        }

        Long sn = getSnFromRecord(record);
        publisher.publishMessage( sn, 1L, record);

        nextExpectedSN = sn +1;

        executorService.execute(this::processNextMessage);

    }

    private void openFile(){
        try {
            reader = new BufferedReader(new FileReader("ExternalMessage.txt"));

        } catch (FileNotFoundException e) {
            String errorMessage = "Failed to open the file. Error ["+e.getMessage()+"]";
            logger.error(errorMessage);
            throw  new RuntimeException(errorMessage);
        }
    }

    private void seekToPosition(Long nextExpectedSN){
        Long sn = 0L;

        while ( sn + 1 < nextExpectedSN){ // we don't want to read next exptected record
            String nextRecord = getNextRecord();
            sn = getSnFromRecord(nextRecord);
        }

    }
    private Long getSnFromRecord(String record){

        Long sn = -1L;
        sn = Long. parseLong(record.substring(0,6));

        return sn;

    }

    private String getNextRecord(){
        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line;
    }
}
