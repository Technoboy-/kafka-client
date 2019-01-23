package com.owl.kafka.client.proxy.zookeeper;

import com.owl.kafka.client.serializer.SerializerImpl;
import com.owl.kafka.client.util.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @Author: Tboy
 */
public class KafkaZookeeperConfig {

    public static String getBrokerIds(String zookeeperServers, String namespace){
        if(StringUtils.isBlank(zookeeperServers)){
            throw new IllegalArgumentException("ZookeeperServers should not be empty");
        }
        StringBuilder builder = new StringBuilder();
        ZookeeperClient zookeeperClient = new ZookeeperClient(zookeeperServers, namespace, 30000, 15000);
        try {
            zookeeperClient.start();
            final String brokerIds = "/brokers/ids";
            List<String> children = zookeeperClient.getChildren(brokerIds);
            for(String path : children){
                byte[] data = zookeeperClient.getData(brokerIds + "/" + path);
                BrokerId brokerId = (BrokerId) SerializerImpl.getFastJsonSerializer().deserialize(data, BrokerId.class);
                builder.append(brokerId.getHost() + ":" + brokerId.getPort()).append(",");
            }
            String serverList = builder.toString();
            if(!StringUtils.isBlank(serverList)){
                return serverList.substring(0, serverList.lastIndexOf(","));
            } else{
                throw new IllegalArgumentException("parse kafka broker list, but empty");
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            zookeeperClient.close();
        }
    }

    static class BrokerId implements Serializable {

        private Map<String, String> listener_security_protocol_map;

        private String endpoints;

        private String host;

        private int port;

        private int version;

        private String timestamp;

        public Map<String, String> getListener_security_protocol_map() {
            return listener_security_protocol_map;
        }

        public void setListener_security_protocol_map(Map<String, String> listener_security_protocol_map) {
            this.listener_security_protocol_map = listener_security_protocol_map;
        }

        public String getEndpoints() {
            return endpoints;
        }

        public void setEndpoints(String endpoints) {
            this.endpoints = endpoints;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "BrokerId{" +
                    "listener_security_protocol_map=" + listener_security_protocol_map +
                    ", endpoints='" + endpoints + '\'' +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", version=" + version +
                    ", timestamp='" + timestamp + '\'' +
                    '}';
        }
    }
}
