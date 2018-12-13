package com.tt.kafka.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author: Tboy
 */
public class CallerWaitPolicy implements RejectedExecutionHandler {

    private BlockingQueue<Runnable> queue;

    public CallerWaitPolicy(BlockingQueue<Runnable> queue){
        this.queue = queue;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        try {
            queue.put(r);
        } catch (InterruptedException ie){
        }
    }
}
