package com.owl.kafka.client;

import com.owl.kafka.client.service.InvokerPromise;
import com.owl.kafka.client.service.RegistryListener;
import com.owl.kafka.client.service.RegistryService;
import com.owl.kafka.client.transport.Address;
import com.owl.kafka.client.transport.NettyConnector;
import com.owl.kafka.client.transport.Reconnector;
import com.owl.kafka.client.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.client.util.Packets;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.consumer.service.MessageListenerService;
import com.owl.kafka.serializer.SerializerImpl;
import com.sun.xml.internal.bind.v2.runtime.reflect.Lister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;

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

    public Record<byte[], byte[]> view(long msgId){
        InvokerPromise promise = new InvokerPromise(msgId, 5000);
        Collection<Reconnector> reconnectors = nettyConnector.getReconnectors().values();
        for(Reconnector reconnector : reconnectors){
            try {
                reconnector.getConnection().send(Packets.view(msgId));
            } catch (ChannelInactiveException e) {
                //TODO
            }
        }
        Packet result = promise.getResult();
        if(result != null){
            Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(result.getHeader(), Header.class);
            return new Record<>(result.getMsgId(), header.getTopic(), header.getPartition(), header.getOffset(), result.getKey(), result.getValue(), -1);
        }
        return null;
    }

    public void close(){
        this.nettyConnector.close();
        this.registryService.close();
        LOGGER.debug("NettyClient closed");
    }


}
