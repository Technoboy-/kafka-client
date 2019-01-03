package com.tt.kafka.client;

import com.tt.kafka.client.netty.transport.PushTcpClient;
import com.tt.kafka.client.service.*;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.Constants;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Tboy
 */
public class PushServerConnector{

    private final MessageListenerService messageListenerService;

    private final LoadBalancePolicy<Address> loadBalancePolicy;

    private final RegistryService registryService;

    private final PushConfigs pushConfigs;

    private final ConcurrentHashMap<Address, PushTcpClient> clients = new ConcurrentHashMap<>();

    public PushServerConnector(MessageListenerService messageListenerService){
        this.messageListenerService = messageListenerService;
        this.pushConfigs = new PushConfigs(false);
        this.registryService = new RegistryService(pushConfigs);
        this.registryService.addListener(new RegistryListener<PushTcpClient>() {
            @Override
            public void onSubscribe(String path) {
                //NOP
            }

            @Override
            public void onRegister(RegisterMetadata<PushTcpClient> metadata) {
                PushTcpClient client = metadata.getRef();
                PushTcpClient pre = clients.get(metadata.getAddress());
                if(pre != null){
                    pre.close();
                }
                clients.put(metadata.getAddress(), client);
            }
        });
        registryService.subscribe(String.format(Constants.ZOOKEEPER_PROVIDERS, pushConfigs.getClientTopic()));
        this.loadBalancePolicy = new RoundRobinPolicy(registryService);
    }

    public void start(){
        for(int i = 1; i <= this.pushConfigs.getClientParallelism(); i ++){
            PushTcpClient pushTcpClient = new PushTcpClient(registryService, messageListenerService);
            Address provider = loadBalancePolicy.get();
            pushTcpClient.connect(provider.getHost(), provider.getPort());
        }
    }

    public void close(){
        registryService.close();
        for(PushTcpClient client : clients.values()){
            client.close();
        }
    }
}
