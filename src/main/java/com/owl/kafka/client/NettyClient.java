package com.owl.kafka.client;

import com.owl.kafka.client.service.RegistryListener;
import com.owl.kafka.client.service.RegistryService;
import com.owl.kafka.client.transport.Address;
import com.owl.kafka.client.transport.NettyConnector;
import com.owl.kafka.consumer.service.MessageListenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @Author: Tboy
 */
public class NettyClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    private final RegistryService registryService;

    private final NettyConnector nettyConnector;

    public NettyClient(MessageListenerService messageListenerService){
        this.nettyConnector = new NettyConnector(messageListenerService);
        this.registryService = new RegistryService();
        this.registryService.addListener(new RegistryListener() {
            @Override
            public void onChange(Address address, Event event) {
                switch (event){
                    case ADD:
                        nettyConnector.connect(new InetSocketAddress(address.getHost(), address.getPort()), true);
                        break;
                    case DELETE:
                        nettyConnector.disconnect(new InetSocketAddress(address.getHost(), address.getPort()));
                        break;
                }
            }
        });
        this.registryService.subscribe(String.format(ClientConfigs.I.ZOOKEEPER_PROVIDERS, ClientConfigs.I.getTopic()));
    }

    public void start(){
        LOGGER.debug("NettyClient started");
    }

    public void close(){
        this.nettyConnector.close();
        this.registryService.close();
        LOGGER.debug("NettyClient closed");
    }


}
