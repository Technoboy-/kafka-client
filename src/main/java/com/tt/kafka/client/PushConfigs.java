package com.tt.kafka.client;

import com.tt.kafka.util.Constants;
import com.tt.kafka.util.Preconditions;
import com.tt.kafka.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

/**
 * @Author: Tboy
 */
public class PushConfigs {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushConfigs.class);

    private static final String CLIENT_CONFIG_FILE = "push_client.properties";

    private static final String SERVER_CONFIG_FILE = "push_server.properties";

    private final Properties properties;

    private final boolean serverSide;

    public PushConfigs(boolean serverSide){
        this.properties = new Properties();
        this.serverSide = serverSide;
        load(serverSide ? SERVER_CONFIG_FILE : CLIENT_CONFIG_FILE);
        checkNotEmtpy();
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
            toSystemProperties();
        } catch (IOException ex) {
            LOGGER.error("error", ex);
            throw new RuntimeException(ex);
        }
    }

    private void toSystemProperties(){
        Enumeration<Object> keys = properties.keys();
        while(keys.hasMoreElements()){
            String key = keys.nextElement().toString();
            System.setProperty(key, properties.get(key).toString());
        }

    }

    private void checkNotEmtpy(){
        Preconditions.checkArgument(!StringUtils.isBlank(getServerTopic()), "topic should not be empty");
        //
        if(serverSide){
            Preconditions.checkArgument(!StringUtils.isBlank(getServerGroupId()), "groupId should not be empty");
        }

    }

    public String getClientTopic() {
        return System.getProperty(Constants.PUSH_CLIENT_TOPIC, properties.getProperty(Constants.PUSH_CLIENT_TOPIC));
    }

    public String getZookeeperNamespace(){
        return System.getProperty(Constants.ZOOKEEPER_NAMESPACE, properties.getProperty(Constants.ZOOKEEPER_NAMESPACE));
    }

    public String getZookeeperServerList(){
        return System.getProperty(Constants.ZOOKEEPER_SERVER_LIST, properties.getProperty(Constants.ZOOKEEPER_SERVER_LIST));
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

}
