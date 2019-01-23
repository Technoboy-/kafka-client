package com.owl.kafka.client.producer;


/**
 * @Author: Tboy
 */
public interface Callback {

    void onCompletion(SendResult result, Exception exception);
}
