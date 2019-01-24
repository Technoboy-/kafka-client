package com.owl.kafka.client.proxy;

import com.owl.kafka.client.util.Constants;
import com.owl.kafka.client.util.Preconditions;
import com.owl.kafka.client.util.StringUtils;

/**
 * @Author: Tboy
 */
public class ClientConfigs extends ConfigLoader{

    static final String CLIENT_TOPIC = "client.topic";

    static final String CLIENT_WORKER_NUM = "client.worker.num";

    static final String CLIENT_PARALLELISM_NUM = "client.parallelism.num";

    static final String CLIENT_PROCESS_QUEUE_SIZE = "client.process.queue.size";

    static final String CLIENT_CONSUME_BATCH_SIZE = "client.consume.batch.size";

    static final  String CLIENT_CONFIG_FILE = "proxy_client.properties";

    public static ClientConfigs I = new ClientConfigs(CLIENT_CONFIG_FILE);

    private ClientConfigs(String fileName){
        super(fileName);
    }

    protected void afterLoad(){
        Preconditions.checkArgument(!StringUtils.isBlank(getTopic()), "topic should not be empty");
    }

    public String getTopic(){
        return get(CLIENT_TOPIC);
    }

    public int getWorkerNum(){
        return getInt(CLIENT_WORKER_NUM, Constants.CPU_SIZE);
    }

    public int getParallelismNum(){
        return getInt(CLIENT_PARALLELISM_NUM, 1);
    }

    public int getConsumeBatchSize(){
        return getInt(CLIENT_CONSUME_BATCH_SIZE, 2);
    }

    public int getProcessQueueSize(){
        return getInt(CLIENT_PROCESS_QUEUE_SIZE, 1000);
    }

}
