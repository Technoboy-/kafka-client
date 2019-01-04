package com.tt.kafka.client.service;

import com.tt.kafka.client.transport.Address;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class RegisterMetadata<T> implements Serializable {

    private String path;

    private Address address;

    private T ref;

    public RegisterMetadata(){
        //
    }

    public RegisterMetadata(String path, Address address, T ref){
        this.path = path;
        this.address = address;
        this.ref = ref;
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

    public T getRef() {
        return ref;
    }

    public void setRef(T ref) {
        this.ref = ref;
    }

    @Override
    public String toString() {
        return "RegisterMetadata{" +
                "path='" + path + '\'' +
                ", address=" + address +
                ", ref=" + ref +
                '}';
    }
}
