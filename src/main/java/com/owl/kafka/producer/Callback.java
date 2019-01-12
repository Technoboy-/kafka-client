package com.owl.kafka.producer;


/**
 * @Author: Tboy
 */
public interface Callback {

    void onCompletion(SendResult result, Exception exception);
}
