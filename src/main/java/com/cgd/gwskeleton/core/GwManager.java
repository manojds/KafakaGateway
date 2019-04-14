package com.cgd.gwskeleton.core;

import com.cgd.gwskeleton.external_interface.ExternalConnector;
import com.cgd.gwskeleton.publisher.DataPublisherImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class GwManager {

    private ExternalConnector externalConnector;
    private DataPublisherImpl dataPublisherImpl;

    Logger logger = LoggerFactory.getLogger(GwManager.class);

    @Autowired
    public GwManager(ExternalConnector externalConnector, DataPublisherImpl dataPublisherImpl){
        this.externalConnector = externalConnector;
        this.dataPublisherImpl = dataPublisherImpl;
        logger.info("GwManager Created");
    }


    @EventListener
    public void handleContextRefresh(ContextRefreshedEvent event) {

        logger.info("GwManager Refreshed  event received..");
        dataPublisherImpl.start();
        externalConnector.start(dataPublisherImpl);
    }
}
