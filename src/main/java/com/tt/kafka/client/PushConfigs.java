package com.tt.kafka.client;

import com.tt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * @Author: Tboy
 */
public class PushConfigs {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushConfigs.class);

    private static final String CLIENT_CONFIG_FILE = "push_client.properties";

    private static final String SERVER_CONFIG_FILE = "push_server.properties";

    private final Properties properties;

    public PushConfigs(boolean serverSide){
        this.properties = new Properties();
        load(serverSide ? SERVER_CONFIG_FILE : CLIENT_CONFIG_FILE);
    }

    private void load(String file){
        InputStream fis = null;
        try {
            URL resource = PushConfigs.class.getClassLoader().getResource(file);
            if(resource == null){
                resource = PushConfigs.class.getResource(file);
            }
            if(resource != null){
                fis = resource.openStream();
            }
            if(fis == null){
                fis = new FileInputStream(new File(file));
            }
        } catch (Exception ex) {
            LOGGER.error("error", ex);
        }
        if(fis == null){
            throw new RuntimeException(file + " not found");
        }
        try {
            properties.load(fis);
        } catch (IOException ex) {
            LOGGER.error("error", ex);
            throw new RuntimeException(ex);
        }
    }

    public String getClientTopic() {
        return System.getProperty(Constants.PUSH_CLIENT_TOPIC, properties.getProperty(Constants.PUSH_CLIENT_TOPIC));
    }

    public int getClientWorkerNum(){
        String workerNum = System.getProperty(Constants.PUSH_CLIENT_WORKER_NUM, properties.getProperty(Constants.PUSH_CLIENT_WORKER_NUM, String.valueOf(Constants.CPU_SIZE + 1)));
        return Integer.valueOf(workerNum);
    }

    public String getZookeeperServerList(){
        return System.getProperty(Constants.ZOOKEEPER_SERVER_LIST, properties.getProperty(Constants.ZOOKEEPER_SERVER_LIST));
    }

    public int getZookeeperSessionTimeoutMs(){
        String sessionTimeoutMs = System.getProperty(Constants.ZOOKEEPER_SESSION_TIMEOUT_MS, properties.getProperty(Constants.ZOOKEEPER_SESSION_TIMEOUT_MS, "60000"));
        return Integer.valueOf(sessionTimeoutMs);
    }

    public int getZookeeperConnectionTimeoutMs(){
        String connectionTimeoutMs =  System.getProperty(Constants.ZOOKEEPER_CONNECTION_TIMEOUT_MS, properties.getProperty(Constants.ZOOKEEPER_CONNECTION_TIMEOUT_MS, "15000"));
        return Integer.valueOf(connectionTimeoutMs);
    }

    public int getServerBossNum(){
        String bossNum = System.getProperty(Constants.PUSH_SERVER_BOSS_NUM, properties.getProperty(Constants.PUSH_SERVER_BOSS_NUM, "1"));
        return Integer.valueOf(bossNum);
    }

    public int getServerWorkerNum(){
        String workerNum = System.getProperty(Constants.PUSH_SERVER_WORKER_NUM, properties.getProperty(Constants.PUSH_SERVER_WORKER_NUM, String.valueOf(Constants.CPU_SIZE)));
        return Integer.valueOf(workerNum);
    }

    public int getServerPort() {
        String port = System.getProperty(Constants.PUSH_SERVER_PORT, properties.getProperty(Constants.PUSH_SERVER_PORT, "10666"));
        return Integer.valueOf(port);
    }

    public String getServerGroupId() {
        return System.getProperty(Constants.PUSH_SERVER_GROUP_ID, properties.getProperty(Constants.PUSH_SERVER_GROUP_ID));
    }

    public String getServerTopic() {
        return System.getProperty(Constants.PUSH_SERVER_TOPIC, properties.getProperty(Constants.PUSH_SERVER_TOPIC));
    }

    public String getServerKafkaServerList() {
        return System.getProperty(Constants.PUSH_SERVER_KAFKA_SERVER_LIST, properties.getProperty(Constants.PUSH_SERVER_KAFKA_SERVER_LIST));
    }

    public int getServerQueueSize(){
        String queueSize = System.getProperty(Constants.PUSH_SERVER_QUEUE_SIZE, properties.getProperty(Constants.PUSH_SERVER_QUEUE_SIZE, "100"));
        return Integer.valueOf(queueSize);
    }

}
