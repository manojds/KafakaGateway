package com.cgd.gwskeleton.config;

import com.cgd.gwskeleton.external_interface.ExternalConnector;
import com.cgd.gwskeleton.external_interface.ExternalConnectorImpl;
import com.cgd.gwskeleton.publisher.DataPublisherImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    @Bean
    DataPublisherImpl dataPublisher(GatewayConfigs gatewayConfigs){
        return new DataPublisherImpl(gatewayConfigs);
    }

    @Bean
    ExternalConnector externalConnector(GatewayConfigs gatewayConfigs){
        return new ExternalConnectorImpl();
    }

    @Bean
    GatewayConfigs gatewayConfigs(){
        return new GatewayConfigs();
    }


}
