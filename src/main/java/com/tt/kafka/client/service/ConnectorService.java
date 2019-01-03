package com.tt.kafka.client.service;

import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.client.PushClient;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;

/**
 * @Author: Tboy
 */
public class ConnectorService {

    private final MessageListenerService messageListenerService;

    private final DiscoveryService discoveryService;

    private final LoadBalancePolicy<Address> loadBalancePolicy;

    public ConnectorService(DiscoveryService discoveryService, MessageListenerService messageListenerService){
        this.discoveryService = discoveryService;
        this.loadBalancePolicy = new RoundRobinPolicy(discoveryService);
        this.messageListenerService = messageListenerService;
    }

    public void start(){
        PushClient pushClient = new PushClient(messageListenerService);
        Address provider = loadBalancePolicy.get();
        Channel channel = pushClient.connect(provider.getHost(), provider.getPort());
        RegisterMetadata metadata = toMetadata(channel);
        discoveryService.register(metadata);
    }

    private RegisterMetadata toMetadata(Channel channel){
        if(channel.localAddress() instanceof InetSocketAddress){
            InetSocketAddress address = (InetSocketAddress)channel.localAddress();
            RegisterMetadata metadata = new RegisterMetadata();
            metadata.setTopic(discoveryService.getClientConfigs().getTopic());
            Address client = new Address(address.getHostName(), address.getPort());
            metadata.setAddress(client);
            return metadata;
        }
        return null;
    }

    public void close(){
        discoveryService.close();
    }
}
