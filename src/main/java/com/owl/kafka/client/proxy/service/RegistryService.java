package com.owl.kafka.client.proxy.service;

import com.owl.kafka.client.proxy.ClientConfigs;
import com.owl.kafka.client.proxy.transport.Address;
import com.owl.kafka.client.proxy.zookeeper.ZookeeperClient;
import com.owl.kafka.client.util.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @Author: Tboy
 */
public class RegistryService implements PathChildrenCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegistryService.class);

    private final CopyOnWriteArraySet<Address> providers = new CopyOnWriteArraySet<>();

    private final ZookeeperClient zookeeperClient;

    private final List<RegistryListener> listeners = new ArrayList<>();

    private PathChildrenCache pathChildrenCache;

    public RegistryService(ZookeeperClient zookeeperClient){
        this.zookeeperClient = zookeeperClient;
        this.zookeeperClient.start();
    }

    public void subscribe(String path){
        this.pathChildrenCache = new PathChildrenCache(this.zookeeperClient.getClient(), path, false);
        this.pathChildrenCache.getListenable().addListener(this);
        try {
            this.pathChildrenCache.start();
        } catch (Exception ex) {
            LOGGER.error("subscribe service {} error {}", path, ex);
            throw new RuntimeException(ex);
        }
    }

    public void addListener(RegistryListener listener){
        listeners.add(listener);
    }

    public void register(RegisterMetadata metadata){
        try {
            if(!zookeeperClient.checkExists(metadata.getPath())){
                zookeeperClient.createPersistent(metadata.getPath());
            }
            zookeeperClient.createEPhemeral(metadata.getPath() +
                    String.format(ClientConfigs.I.ZOOKEEPER_PROVIDER_CONSUMER_NODE, metadata.getAddress().getHost(), metadata.getAddress().getPort()), new ZookeeperClient.BackgroundCallback() {
                @Override
                public void complete() {
                    //NOP
                }
            });
        } catch (Exception ex) {
            LOGGER.error("register service {} error {}", metadata, ex);
            throw new RuntimeException(ex);
        }
    }

    public void unregister(RegisterMetadata metadata){
        try {
            zookeeperClient.delete(metadata.getPath() + String.format(ClientConfigs.I.ZOOKEEPER_PROVIDER_CONSUMER_NODE, metadata.getAddress().getHost(), metadata.getAddress().getPort()));
        } catch (KeeperException.NoNodeException ex) {
            //ignore;
        } catch (Exception ex) {
            LOGGER.error("unregister service {} error {}", metadata, ex);
        }
    }

    public void close(){
        try {
            if(this.pathChildrenCache != null){
                this.pathChildrenCache.close();
            }
        } catch (IOException e) {
            //
        }
    }

    private Address parse(String child){
        Address address = null;
        if(!StringUtils.isBlank(child) && child.contains(":") && child.split(":").length == 2){
            String host = child.split(":")[0];
            int port = Integer.valueOf(child.split(":")[1]);
            address = new Address(host, port);
        }
        return address;
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()){
            case CHILD_ADDED:
                String addPath = event.getData().getPath();
                LOGGER.debug("add node path : {}, value : {} ", addPath);
                String childAddPath = addPath.substring(addPath.lastIndexOf("/") + 1);
                Address addAddress = parse(childAddPath);
                if(addAddress != null && providers.add(addAddress)){
                    for(RegistryListener listener : listeners){
                        listener.onChange(addAddress, RegistryListener.Event.ADD);
                    }
                }
                break;
            case CHILD_REMOVED:
                String removePath = event.getData().getPath();
                LOGGER.debug("remove node path : {}, value : {} ", removePath);
                String childRemovePath = removePath.substring(removePath.lastIndexOf("/") + 1);
                Address removeAddress = parse(childRemovePath);
                if(removeAddress != null && providers.remove(removeAddress)){
                    for(RegistryListener listener : listeners){
                        listener.onChange(removeAddress, RegistryListener.Event.DELETE);
                    }
                }
                break;
            default:
                LOGGER.debug("state ", event.getType().name());
                break;
        }
    }
}
