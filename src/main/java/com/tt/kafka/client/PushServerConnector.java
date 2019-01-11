package com.tt.kafka.client;

import com.tt.kafka.client.service.RegistryListener;
import com.tt.kafka.client.service.RegistryService;
import com.tt.kafka.client.transport.Address;
import com.tt.kafka.client.transport.NettyClient;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @Author: Tboy
 */
public class PushServerConnector{

    private static final Logger LOGGER = LoggerFactory.getLogger(PushServerConnector.class);

    private final RegistryService registryService;

    private final PushConfigs pushConfigs;

    private final NettyClient nettyClient;

    public PushServerConnector(MessageListenerService messageListenerService){
        this.nettyClient = new NettyClient(messageListenerService);
        this.pushConfigs = new PushConfigs(false);
        this.registryService = new RegistryService();
        this.registryService.addListener(new RegistryListener() {

            @Override
            public void onChange(Address address, Event event) {
                switch (event){
                    case ADD:
                        nettyClient.connect(new InetSocketAddress(address.getHost(), address.getPort()));
                        break;
                    case DELETE:
                        nettyClient.disconnect(new InetSocketAddress(address.getHost(), address.getPort()));
                        break;
                }
            }
        });
        this.registryService.subscribe(String.format(Constants.ZOOKEEPER_PROVIDERS, pushConfigs.getClientTopic()));
    }

    public void start(){
        LOGGER.debug("PushServerConnector started");
    }

    public void close(){
        this.nettyClient.close();
        this.registryService.close();
        LOGGER.debug("PushServerConnector closed");
    }


}
