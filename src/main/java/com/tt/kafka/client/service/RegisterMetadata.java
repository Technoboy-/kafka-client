package com.tt.kafka.client.service;

import java.io.Serializable;

/**
 * @Author: Tboy
 */
public class RegisterMetadata implements Serializable {

    private String topic;

    private Address address;

    public RegisterMetadata(){
        //
    }

    public RegisterMetadata(String topic, Address address){
        this.topic = topic;
        this.address = address;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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
                "topic='" + topic + '\'' +
                ", address=" + address +
                '}';
    }
}
