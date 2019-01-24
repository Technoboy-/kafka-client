package com.owl.kafka.client.proxy;

import com.owl.kafka.client.proxy.service.InvokerPromise;
import com.owl.kafka.client.proxy.service.RegistryListener;
import com.owl.kafka.client.proxy.service.RegistryService;
import com.owl.kafka.client.proxy.transport.Address;
import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.NettyClient;
import com.owl.kafka.client.proxy.transport.ConnectionManager;
import com.owl.kafka.client.proxy.transport.message.Message;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.util.MessageCodec;
import com.owl.kafka.client.proxy.util.Packets;
import com.owl.kafka.client.proxy.zookeeper.ZookeeperClient;
import com.owl.kafka.client.consumer.Record;
import com.owl.kafka.client.consumer.service.MessageListenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @Author: Tboy
 */
public class DefaultPushMessageImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPushMessageImpl.class);

    private final RegistryService registryService;

    private final NettyClient nettyClient;

    private final ZookeeperClient zookeeperClient;

    private final String serverList = ClientConfigs.I.getZookeeperServerList();

    private final int sessionTimeoutMs = ClientConfigs.I.getZookeeperSessionTimeoutMs();

    private final int connectionTimeoutMs = ClientConfigs.I.getZookeeperConnectionTimeoutMs();

    public DefaultPushMessageImpl(MessageListenerService messageListenerService){
        this.nettyClient = new NettyClient(messageListenerService);
        this.zookeeperClient = new ZookeeperClient(serverList, sessionTimeoutMs, connectionTimeoutMs);
        this.registryService = new RegistryService(zookeeperClient);
        this.registryService.addListener(new RegistryListener() {
            @Override
            public void onChange(Address address, Event event) {
                switch (event){
                    case ADD:
                        nettyClient.connect(address, true);
                        break;
                    case DELETE:
                        nettyClient.disconnect(address);
                        break;
                }
            }
        });
        this.registryService.subscribe(String.format(ClientConfigs.I.ZOOKEEPER_PROVIDERS, ClientConfigs.I.getTopic()));
    }

    public void start(){
        LOGGER.debug("DefaultPushMessageImpl started");
    }

    public Record<byte[], byte[]> view(long msgId){
        Record result = Record.EMPTY;
        try {
            List<String> children = zookeeperClient.getChildren(String.format(ConfigLoader.ZOOKEEPER_CONSUMERS, ClientConfigs.I.getTopic() + "-dlq"));
            ConnectionManager connectionManager = null;
            for(String child : children){
                Address address = Address.parse(child);
                if(address != null){
                    Connection connection = nettyClient.getConnectionManager().getConnection(address);
                    connection.send(Packets.viewReq(msgId));
                    InvokerPromise promise = new InvokerPromise(msgId, 5000);
                    Packet packet = promise.getResult();
                    if(packet != null && !packet.isBodyEmtpy()){
                        Message message = MessageCodec.decode(packet.getBody());
                        return new Record<>(message.getHeader().getMsgId(), message.getHeader().getTopic(),
                                message.getHeader().getPartition(), message.getHeader().getOffset(), message.getKey(), message.getValue(), -1);
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.error("view msgId : {}, error", msgId, ex);
        }
        return result;
    }

    public void close(){
        this.nettyClient.close();
        this.registryService.close();
        this.zookeeperClient.close();
        LOGGER.debug("DefaultPushMessageImpl closed");
    }

}
