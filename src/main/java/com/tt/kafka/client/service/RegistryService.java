package com.tt.kafka.client.service;

import com.tt.kafka.client.PushConfigs;
import com.tt.kafka.client.transport.Address;
import com.tt.kafka.client.zookeeper.ZookeeperClient;
import com.tt.kafka.util.CollectionUtils;
import com.tt.kafka.util.Constants;
import com.tt.kafka.util.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class RegistryService implements TreeCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegistryService.class);

    private final List<Address> providers = new ArrayList<>();

    private final ZookeeperClient zookeeperClient;

    private TreeCache treeCache;

    private final PushConfigs clientConfigs;

    private final List<RegistryListener> listeners = new ArrayList<>();

    public RegistryService(PushConfigs clientConfigs){
        this.clientConfigs = clientConfigs;
        this.zookeeperClient = new ZookeeperClient(clientConfigs);
        this.zookeeperClient.start();
    }

    public void subscribe(String path){
        for(RegistryListener listener : listeners){
            listener.onSubscribe(path);
        }
        parseProviders(path);
        this.treeCache = new TreeCache(this.zookeeperClient.getClient(), path);
        this.treeCache.getListenable().addListener(this);
        try {
            this.treeCache.start();
        } catch (Exception ex) {
            LOGGER.error("subscribe service {} error {}", path, ex);
            throw new RuntimeException(ex);
        }
    }

    public void addListener(RegistryListener listener){
        listeners.add(listener);
    }

    public void register(RegisterMetadata metadata){
        for(RegistryListener listener : listeners){
            listener.onRegister(metadata);
        }
        try {
            if(!zookeeperClient.checkExists(metadata.getPath())){
                zookeeperClient.createPersistent(metadata.getPath());
            }
            zookeeperClient.createEPhemeral(metadata.getPath() +
                    String.format(Constants.ZOOKEEPER_PROVIDER_CONSUMER_NODE, metadata.getAddress().getHost(), metadata.getAddress().getPort()), new ZookeeperClient.BackgroundCallback() {
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

    public void destroy(RegisterMetadata metadata){
        for(RegistryListener listener : listeners){
            listener.onDestroy(metadata);
        }
        try {
            zookeeperClient.delete(metadata.getPath() + String.format(Constants.ZOOKEEPER_PROVIDER_CONSUMER_NODE, metadata.getAddress().getHost(), metadata.getAddress().getPort()));
        } catch (Exception ex) {
            LOGGER.error("destroy service {} error {}", metadata, ex);
        }
    }

    public List<Address> getProviders() {
        return providers;
    }


    public PushConfigs getClientConfigs() {
        return clientConfigs;
    }

    public void close(){
        this.treeCache.close();
        this.zookeeperClient.close();
    }

    public void parseProviders(String path){
        try {
            List<String> children = this.zookeeperClient.getChildren(path);
            LOGGER.debug("get path :{} , children : {} ", path, children);
            if(!CollectionUtils.isEmpty(children)){
                for(String child : children){
                    parseAndAdd(child, providers);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("parseProviders , path : {} error : {}", path, ex);
        }
    }

    private void parseAndAdd(String node, List<Address> providers){
        Address address = null;
        if(node.contains(":") && node.split(":").length == 2){
            String host = node.split(":")[0];
            int port = Integer.valueOf(node.split(":")[1]);
            address = new Address(host, port);
        }
        if(address != null){
            providers.add(address);
        }
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        switch (event.getType()){
            case NODE_ADDED:
                String nodeAddValue = StringUtils.getString(event.getData().getData());
                LOGGER.debug("node added : {} ", nodeAddValue);
                parseAndAdd(nodeAddValue, providers);
                break;
            case NODE_REMOVED:
                String nodeRemovedValue = StringUtils.getString(event.getData().getData());
                LOGGER.debug("node removed : {} ", nodeRemovedValue);
                parseAndAdd(nodeRemovedValue, providers);
                break;
            case NODE_UPDATED:
                String nodeUpdatedValue = StringUtils.getString(event.getData().getData());
                LOGGER.debug("node updated : {} ", nodeUpdatedValue);
                parseAndAdd(nodeUpdatedValue, providers);
                break;
                default:
                    //just ignore
                    break;
        }
    }
}
