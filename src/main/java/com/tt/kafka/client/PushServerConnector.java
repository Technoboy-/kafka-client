package com.tt.kafka.client;

import com.tt.kafka.client.service.LoadBalancePolicy;
import com.tt.kafka.client.service.RegistryService;
import com.tt.kafka.client.service.RoundRobinPolicy;
import com.tt.kafka.client.transport.Address;
import com.tt.kafka.client.transport.PushTcpClient;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.Constants;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class PushServerConnector{

    private final MessageListenerService messageListenerService;

    private final LoadBalancePolicy<Address> loadBalancePolicy;

    private final RegistryService registryService;

    private final PushConfigs pushConfigs;

    private final List<PushTcpClient> clients;

    public PushServerConnector(MessageListenerService messageListenerService){
        this.messageListenerService = messageListenerService;
        this.pushConfigs = new PushConfigs(false);
        this.registryService = new RegistryService(pushConfigs);
        registryService.subscribe(String.format(Constants.ZOOKEEPER_PROVIDERS, pushConfigs.getClientTopic()));
        this.loadBalancePolicy = new RoundRobinPolicy(registryService);
        this.clients = new ArrayList<>(this.pushConfigs.getClientParallelism());
    }

    public void start(){
        for(int i = 1; i <= this.pushConfigs.getClientParallelism(); i ++){
            PushTcpClient pushTcpClient = new PushTcpClient(registryService, messageListenerService);
            Address provider = loadBalancePolicy.get();
            pushTcpClient.connect(new InetSocketAddress(provider.getHost(), provider.getPort()));
        }
    }

    public void close(){
        registryService.close();
        for(PushTcpClient client : clients){
            client.close();
        }
    }
}
