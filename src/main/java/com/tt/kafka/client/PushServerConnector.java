package com.tt.kafka.client;

import com.tt.kafka.client.service.RegistryService;
import com.tt.kafka.client.transport.NettyClient;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class PushServerConnector{

    private static final Logger LOGGER = LoggerFactory.getLogger(PushServerConnector.class);

    private final MessageListenerService messageListenerService;

    private final RegistryService registryService;

    private final PushConfigs pushConfigs;

    private final List<NettyClient> clients;

    public PushServerConnector(MessageListenerService messageListenerService){
        this.messageListenerService = messageListenerService;
        this.pushConfigs = new PushConfigs(false);
        this.registryService = new RegistryService(pushConfigs);
        this.registryService.subscribe(String.format(Constants.ZOOKEEPER_PROVIDERS, pushConfigs.getClientTopic()));
        this.clients = new ArrayList<>(this.pushConfigs.getClientParallelism());
    }

    public void start(){
        for(int i = 1; i <= this.pushConfigs.getClientParallelism(); i ++){
            NettyClient nettyClient = new NettyClient(registryService, messageListenerService);
            nettyClient.connect();
            clients.add(nettyClient);
        }
    }

    public void close(){
        registryService.close();
        for(NettyClient client : clients){
            client.close();
        }
    }
}
