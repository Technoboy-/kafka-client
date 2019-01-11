package com.tt.kafka.client;

import com.tt.kafka.client.service.RegistryListener;
import com.tt.kafka.client.service.RegistryService;
import com.tt.kafka.client.transport.Address;
import com.tt.kafka.client.transport.NettyConnector;
import com.tt.kafka.client.util.SystemPropertiesUtils;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @Author: Tboy
 */
public class NettyClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    private final PushConfigs pushConfigs = new PushConfigs(false);

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
                        nettyConnector.connect(new InetSocketAddress(address.getHost(), address.getPort()));
                        break;
                    case DELETE:
                        nettyConnector.disconnect(new InetSocketAddress(address.getHost(), address.getPort()));
                        break;
                }
            }
        });
        this.registryService.subscribe(String.format(Constants.ZOOKEEPER_PROVIDERS, SystemPropertiesUtils.get(Constants.PUSH_CLIENT_TOPIC)));
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
