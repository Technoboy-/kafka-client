package com.owl.kafka.client.proxy.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class DefaultRetryPolicy implements RetryPolicy {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRetryPolicy.class);

    private final int retryCountReplica;

    private int retryCount;

    private long retryPeriod;

    public DefaultRetryPolicy(){
        this(Integer.MAX_VALUE, 30);
    }

    public DefaultRetryPolicy(int retryCount, long retryPeriod){
        this.retryCount = retryCount;
        this.retryCountReplica = retryCount;
        this.retryPeriod = retryPeriod;
    }

    @Override
    public boolean allowRetry() throws InterruptedException{
        if(retryCount > 0){
            if(LOGGER.isTraceEnabled()){
                LOGGER.trace("Thread " + Thread.currentThread().getName() + " is retrying to get proxy ...");
            }
            Thread.sleep(retryPeriod);
            retryCount--;
            return true;
        }
        return false;
    }

    @Override
    public void reset() {
        retryCount = retryCountReplica;
    }
}
