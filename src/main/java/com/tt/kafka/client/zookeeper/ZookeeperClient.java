package com.tt.kafka.client.zookeeper;

import com.tt.kafka.client.PushConfigs;
import com.tt.kafka.serializer.SerializerImpl;
import com.tt.kafka.util.Constants;
import com.tt.kafka.util.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;


/**
 * @Author: Tboy
 */
public class ZookeeperClient {

    private final CuratorFramework client;

    public ZookeeperClient(PushConfigs pushConfigs){
        this.client = CuratorFrameworkFactory.builder()
                .namespace(Constants.ZOOKEEPER_PUSH_SERVER_NAMESPACE)
                .connectString(pushConfigs.getZookeeperServerList())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(pushConfigs.getZookeeperSessionTimeoutMs())
                .connectionTimeoutMs(pushConfigs.getZookeeperConnectionTimeoutMs())
                .build();
    }

    public ZookeeperClient(String serverList, String namespace, int sessionTimeoutMs, int connectionTimeoutMs){
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        if(!StringUtils.isBlank(namespace)){
            builder.namespace(namespace);
        }
        this.client = builder
                .connectString(serverList)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .build();
    }

    public void start(){
        this.client.start();
    }

    public List<String> getChildren(String path) throws Exception {
        checkState();
        return this.client.getChildren().forPath(path);
    }

    public byte[] getData(String path) throws Exception{
        checkState();
        return this.client.getData().forPath(path);
    }

    public boolean checkExists(String path) throws Exception {
        checkState();
        Stat stat = this.client.checkExists().forPath(path);
        return stat != null;
    }

    public void createPersistent(String path) throws Exception {
        checkState();
        this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
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

    public void delete(String path) throws Exception{
        checkState();
        this.client.delete().forPath(path);
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
