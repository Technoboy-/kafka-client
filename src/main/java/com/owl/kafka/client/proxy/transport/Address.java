package com.owl.kafka.client.proxy.transport;

import com.owl.kafka.client.util.StringUtils;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class Address implements Serializable {

    public Address(){

    }

    public Address(String host, int port){
        this.host = host;
        this.port = port;
    }

    private String host;
    private int port;

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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Address other = (Address) obj;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (port != other.port)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Address [host=" + host + ", port=" + port + "]";
    }

    public static Address parse(String child){
        Address address = null;
        if(!StringUtils.isBlank(child) && child.contains(":") && child.split(":").length == 2){
            String host = child.split(":")[0];
            int port = Integer.valueOf(child.split(":")[1]);
            address = new Address(host, port);
        }
        return address;
    }
}
