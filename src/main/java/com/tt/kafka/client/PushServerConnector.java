package com.tt.kafka.client;

import com.tt.kafka.client.service.RegisterMetadata;
import com.tt.kafka.client.service.RegistryListener;
import com.tt.kafka.client.service.RegistryService;
import com.tt.kafka.client.transport.Address;
import com.tt.kafka.client.transport.NettyClient;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.Constants;
import com.tt.kafka.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Tboy
 */
public class PushServerConnector{

    private static final Logger LOGGER = LoggerFactory.getLogger(PushServerConnector.class);

    private final Pair<MessageListener, MessageListenerService> pair;

    private final RegistryService registryService;

    private final PushConfigs pushConfigs;

    private final ConcurrentHashMap<Address, NettyClient> clients = new ConcurrentHashMap<>();

    public PushServerConnector(Pair<MessageListener, MessageListenerService> pair){
        this.pair = pair;
        this.pushConfigs = new PushConfigs(false);
        this.registryService = new RegistryService(pushConfigs);
        this.registryService.addListener(new RegistryListener() {
            @Override
            public void onSubscribe(String path) {

            }

            @Override
            public void onRegister(RegisterMetadata metadata) {

            }

            @Override
            public void onDestroy(RegisterMetadata metadata) {

            }

            @Override
            public void onChange(Address address, Event event) {
                switch (event){
                    case ADD:
                        addNettyClient(address);
                        break;
                    case DELETE:
                        NettyClient pre = clients.remove(address);
                        pre.close();
                        break;
                }
            }
        });
        this.registryService.subscribe(String.format(Constants.ZOOKEEPER_PROVIDERS, pushConfigs.getClientTopic()));
    }

    public void start(){
        for(Address address : registryService.getCopyProviders()){
            addNettyClient(address);
        }
    }

    private void addNettyClient(Address address){
        if(!clients.containsKey(address)){
            NettyClient nettyClient = new NettyClient(registryService, pair);
            nettyClient.connect(new InetSocketAddress(address.getHost(), address.getPort()));
            clients.put(address, nettyClient);
        }
    }

    public void close(){
        registryService.close();
        for(NettyClient client : clients.values()){
            client.close();
        }
    }


}
