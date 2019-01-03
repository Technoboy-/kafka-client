package com.tt.kafka.client.zookeeper;

import com.tt.kafka.client.PushClientConfigs;
import com.tt.kafka.util.Constants;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.List;


/**
 * @Author: Tboy
 */
public class ZookeeperClient {

    private final CuratorFramework client;

    public ZookeeperClient(PushClientConfigs clientConfigs){
        this.client = CuratorFrameworkFactory.builder()
                .namespace(Constants.ZOOKEEPER_NAMESPACE)
                .connectString(clientConfigs.getZookeeperServerList())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(clientConfigs.getZookeeperSessionTimeoutMs())
                .connectionTimeoutMs(clientConfigs.getZookeeperConnectionTimeoutMs())
                .build();
    }

    public void start(){
        this.client.start();
    }

    public List<String> getChildren(String path) throws Exception {
        checkState();
        return this.client.getChildren().forPath(path);
    }

    public void createEPhemeral(String path, BackgroundCallback callback) throws Exception {
        checkState();
        this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground(new org.apache.curator.framework.api.BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                if(callback != null){
                    callback.complete();
                }
            }
        }).forPath(path);
    }

    public CuratorFramework getClient() {
        return client;
    }

    private void checkState(){
        CuratorFrameworkState state = this.client.getState();
        switch (state){
            case LATENT:
                throw new RuntimeException("ZookeeperClient not start");
            case STARTED:
                return;
            case STOPPED:
                throw new RuntimeException("ZookeeperClient stop");
        }
    }

    public interface BackgroundCallback{

        void complete();
    }

    public void close(){
        this.client.close();
    }
}
