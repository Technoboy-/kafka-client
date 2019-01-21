package com.owl.kafka.client;

import com.owl.kafka.client.service.*;
import com.owl.kafka.client.transport.Address;
import com.owl.kafka.client.transport.NettyClient;
import com.owl.kafka.client.transport.Reconnector;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.client.util.Packets;
import com.owl.kafka.client.zookeeper.ZookeeperClient;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.consumer.service.MessageListenerService;
import com.owl.kafka.serializer.SerializerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author: Tboy
 */
public class DefaultPullMessageImpl {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPullMessageImpl.class);

    private final RegistryService registryService;

    private final NettyClient nettyClient;

    private final ZookeeperClient zookeeperClient;

    private final PullMessageService pullMessageService;

    private final String serverList = ClientConfigs.I.getZookeeperServerList();

    private final int sessionTimeoutMs = ClientConfigs.I.getZookeeperSessionTimeoutMs();

    private final int connectionTimeoutMs = ClientConfigs.I.getZookeeperConnectionTimeoutMs();

    public DefaultPullMessageImpl(MessageListenerService messageListenerService){
        this.nettyClient = new NettyClient(messageListenerService);
        this.pullMessageService = new PullMessageService(nettyClient);
        this.zookeeperClient = new ZookeeperClient(serverList, sessionTimeoutMs, connectionTimeoutMs);
        this.registryService = new RegistryService(zookeeperClient);
        this.registryService.addListener(new RegistryListener() {
            @Override
            public void onChange(Address address, Event event) {
                switch (event){
                    case ADD:
                        nettyClient.connect(address, true);
                        pullMessageService.pullImmediately(address);
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
        LOGGER.debug("DefaultPullMessageImpl started");

    }

    public Record<byte[], byte[]> view(long msgId){
        try {
            List<String> children = zookeeperClient.getChildren(String.format(ConfigLoader.ZOOKEEPER_CONSUMERS, ClientConfigs.I.getTopic() + "-dlq"));
            Reconnector reconnector = null;
            for(String child : children){
                Address address = Address.parse(child);
                if(address != null){
                    Set<Map.Entry<Address, Reconnector>> entries = nettyClient.getReconnectors().entrySet();
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
        this.pullMessageService.close();
        this.nettyClient.close();
        this.registryService.close();
        this.zookeeperClient.close();
        LOGGER.debug("DefaultPullMessageImpl closed");
    }

}
