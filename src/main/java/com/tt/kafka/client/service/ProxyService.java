package com.tt.kafka.client.service;

import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.client.PushClientConfigs;


/**
 * @Author: Tboy
 */
public class ProxyService{

    private final PushClientConfigs clientConfigs;

    private final DiscoveryService discoveryService;

    private final ConnectorService connectorService;

    public ProxyService(MessageListenerService messageListenerService){
        this.clientConfigs = new PushClientConfigs();
        this.discoveryService = new DiscoveryService(clientConfigs);
        this.connectorService = new ConnectorService(discoveryService, messageListenerService);
    }

    public void start(){
        discoveryService.start();
        connectorService.start();
    }

    public void close(){
        connectorService.close();
        discoveryService.close();
    }
}
