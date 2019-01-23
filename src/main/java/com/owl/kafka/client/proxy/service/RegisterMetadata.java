package com.owl.kafka.client.proxy.service;

import com.owl.kafka.client.proxy.transport.Address;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class RegisterMetadata implements Serializable {

    private String path;

    private Address address;

    public RegisterMetadata(){
        //
    }

    public RegisterMetadata(String path, Address address){
        this.path = path;
        this.address = address;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "RegisterMetadata{" +
                "path='" + path + '\'' +
                ", address=" + address +
                '}';
    }
}
