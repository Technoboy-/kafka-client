package com.owl.kafka.client.proxy;

import com.owl.kafka.client.util.StringUtils;
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
public abstract class ConfigLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientConfigs.class);

    public static String ZOOKEEPER_SERVER_LIST = "zookeeper.server.list";

    public static String ZOOKEEPER_NAMESPACE = "zookeeper.namespace";

    public static String ZOOKEEPER_PROVIDERS = "/%s/providers";

    public static String ZOOKEEPER_CONSUMERS = "/%s/consumers";

    public static String ZOOKEEPER_PROVIDER_CONSUMER_NODE = "/%s:%s";

    public static String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";

    public static String ZOOKEEPER_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms";

    protected final Properties properties = new Properties();

    public ConfigLoader(String fileName){
        load(fileName);
        toSystemProperties();
    }

    protected void load(String fileName){
        InputStream fis = null;
        try {
            URL resource = ConfigLoader.class.getClassLoader().getResource(fileName);
            if(resource == null){
                resource = ConfigLoader.class.getResource(fileName);
            }
            if(resource != null){
                fis = resource.openStream();
            }
            if(fis == null){
                fis = new FileInputStream(new File(fileName));
            }
        } catch (Exception ex) {
            LOGGER.error("error", ex);
        }
        if(fis == null){
            throw new RuntimeException(fileName + " not found");
        }
        try {
            properties.load(fis);
            toSystemProperties();
        } catch (IOException ex) {
            LOGGER.error("error", ex);
            throw new RuntimeException(ex);
        }
        afterLoad();
    }

    protected abstract void afterLoad();

    protected void toSystemProperties(){
        Enumeration<Object> keys = properties.keys();
        while(keys.hasMoreElements()){
            String key = keys.nextElement().toString();
            System.setProperty(key, properties.get(key).toString());
        }
    }

    public String getZookeeperNamespace(){
        return get(ZOOKEEPER_NAMESPACE);
    }

    public String getZookeeperServerList(){
        return get(ZOOKEEPER_SERVER_LIST);
    }

    public int getZookeeperSessionTimeoutMs(){
        return getInt(ZOOKEEPER_SESSION_TIMEOUT_MS, 60000);
    }

    public int getZookeeperConnectionTimeoutMs(){
        return getInt(ZOOKEEPER_CONNECTION_TIMEOUT_MS, 15000);
    }

    protected int getInt(String key, int def){
        String value = get(key);
        if(StringUtils.isBlank(value)){
            return def;
        }
        return Integer.valueOf(value);
    }

    protected long getLong(String key, long def){
        String value = get(key);
        if(StringUtils.isBlank(value)){
            return def;
        }
        return Long.valueOf(value);
    }

    protected String get(String key, String def){
        String value = get(key);
        if(StringUtils.isBlank(value)){
            return def;
        }
        return value;
    }

    protected String get(String key){
        return System.getProperty(key);
    }
}
