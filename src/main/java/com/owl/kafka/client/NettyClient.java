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
import com.owl.kafka.client.zookeeper.ZookeeperClient;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.consumer.service.MessageListenerService;
import com.owl.kafka.serializer.SerializerImpl;
import com.sun.xml.internal.bind.v2.runtime.reflect.Lister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * @Author: Tboy
 */
public class NettyClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    private final RegistryService registryService;

    private final NettyConnector nettyConnector;

    private final ZookeeperClient zookeeperClient;

    private final String serverList = ClientConfigs.I.getZookeeperServerList();

    private final int sessionTimeoutMs = ClientConfigs.I.getZookeeperSessionTimeoutMs();

    private final int connectionTimeoutMs = ClientConfigs.I.getZookeeperConnectionTimeoutMs();

    public NettyClient(MessageListenerService messageListenerService){
        this.nettyConnector = new NettyConnector(messageListenerService);
        this.zookeeperClient = new ZookeeperClient(serverList, sessionTimeoutMs, connectionTimeoutMs);
        this.registryService = new RegistryService(zookeeperClient);
        this.registryService.addListener(new RegistryListener() {
            @Override
            public void onChange(Address address, Event event) {
                switch (event){
                    case ADD:
                        nettyConnector.connect(address, true);
                        break;
                    case DELETE:
                        nettyConnector.disconnect(address);
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
        try {
            List<String> children = zookeeperClient.getChildren(String.format(ConfigLoader.ZOOKEEPER_CONSUMERS, ClientConfigs.I.getTopic() + "-dlq"));
            Reconnector reconnector = null;
            for(String child : children){
                Address address = Address.parse(child);
                if(address != null){
                    Set<Map.Entry<Address, Reconnector>> entries = nettyConnector.getReconnectors().entrySet();
                    for(Map.Entry<Address, Reconnector> entry : entries){
                        if(entry.getKey().equals(address)){
                            reconnector = entry.getValue();
                            break;
                        }
                    }
                }
            }
            if(reconnector != null){
                reconnector.getConnection().send(Packets.view(msgId));
                InvokerPromise promise = new InvokerPromise(msgId, 5000);
                Packet result = promise.getResult();
                if(result != null){
                    Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(result.getHeader(), Header.class);
                    return new Record<>(result.getMsgId(), header.getTopic(), header.getPartition(), header.getOffset(), result.getKey(), result.getValue(), -1);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("view msgId : {}, error", msgId, ex);
        }
        return null;
    }

    public void close(){
        this.nettyConnector.close();
        this.registryService.close();
        this.zookeeperClient.close();
        LOGGER.debug("NettyClient closed");
    }


}
