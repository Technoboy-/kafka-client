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
public class PushClientConfigs {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushClientConfigs.class);

    private static final String CONFIG_FILE = "push_client.properties";

    private final Properties properties;

    public PushClientConfigs(){
        this.properties = new Properties();
        load();
    }

    private void load(){
        InputStream fis = null;
        try {
            URL resource = PushClientConfigs.class.getClassLoader().getResource(CONFIG_FILE);
            if(resource == null){
                resource = PushClientConfigs.class.getResource(CONFIG_FILE);
            }
            if(resource != null){
                fis = resource.openStream();
            }
            if(fis == null){
                fis = new FileInputStream(new File(CONFIG_FILE));
            }
        } catch (Exception ex) {
            LOGGER.error("error", ex);
        }
        if(fis == null){
            throw new RuntimeException(CONFIG_FILE + " not found");
        }
        try {
            properties.load(fis);
        } catch (IOException ex) {
            LOGGER.error("error", ex);
            throw new RuntimeException(ex);
        }
    }

    public String getTopic() {
        return System.getProperty(Constants.PUSH_CLIENT_TOPIC, properties.getProperty(Constants.PUSH_CLIENT_TOPIC));
    }

    public String getZookeeperServerList(){
        return System.getProperty(Constants.ZOOKEEPER_SERVER_LIST, properties.getProperty(Constants.ZOOKEEPER_SERVER_LIST));
    }

    public int getZookeeperSessionTimeoutMs(){
        String sessionTimeoutMs = System.getProperty(Constants.ZOOKEEPER_SESSION_TIMEOUT_MS, properties.getProperty(Constants.ZOOKEEPER_SESSION_TIMEOUT_MS));
        return Integer.getInteger(sessionTimeoutMs, 60 * 1000);
    }

    public int getZookeeperConnectionTimeoutMs(){
        String connectionTimeoutMs =  System.getProperty(Constants.ZOOKEEPER_CONNECTION_TIMEOUT_MS, properties.getProperty(Constants.ZOOKEEPER_CONNECTION_TIMEOUT_MS));
        return Integer.getInteger(connectionTimeoutMs, 15 * 1000);
    }

}
