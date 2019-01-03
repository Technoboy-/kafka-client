package com.tt.kafka.client.service;

import com.tt.kafka.client.PushClientConfigs;
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
public class DiscoveryService implements TreeCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryService.class);

    private final List<Address> providers = new ArrayList<>();

    private final ZookeeperClient zookeeperClient;

    private TreeCache treeCache;

    private final PushClientConfigs clientConfigs;

    public DiscoveryService(PushClientConfigs clientConfigs){
        this.clientConfigs = clientConfigs;
        this.zookeeperClient = new ZookeeperClient(clientConfigs);
        this.zookeeperClient.start();
    }

    public void subscribe(String topic){
        this.treeCache = new TreeCache(this.zookeeperClient.getClient(), topic);
        this.treeCache.getListenable().addListener(this);
        try {
            this.treeCache.start();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void register(RegisterMetadata metadata){
        String consumerPath = String.format(Constants.ZOOKEEPER_CONSUMERS, metadata.getTopic());
        try {
            String path = String.format("/%s:%s", metadata.getAddress().getHost(), metadata.getAddress().getPort());
            zookeeperClient.createEPhemeral(consumerPath + path, new ZookeeperClient.BackgroundCallback(){
                @Override
                public void complete() {
                    //TODO
                    //for monitor use
                }
            });
        } catch (Exception ex){
            LOGGER.error("register service error", ex);
            throw new RuntimeException(ex);
        }
    }

    public List<Address> getProviders() {
        return providers;
    }


    public PushClientConfigs getClientConfigs() {
        return clientConfigs;
    }

    public void start(){
        String providerPath = String.format(Constants.ZOOKEEPER_PROVIDERS, clientConfigs.getTopic());
        parseProviders(providerPath);
        subscribe(providerPath);
    }

    public void close(){
        this.treeCache.close();
        this.zookeeperClient.close();
    }

    public void parseProviders(String topic){
        try {
            List<String> children = this.zookeeperClient.getChildren(topic);
            LOGGER.debug("get children : {} ", children);
            if(!CollectionUtils.isEmpty(children)){
                for(String child : children){
                    parseAndAdd(child, providers);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("getProviders error", ex);
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
