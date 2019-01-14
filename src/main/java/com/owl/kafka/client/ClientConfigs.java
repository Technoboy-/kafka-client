package com.owl.kafka.client;

import com.owl.kafka.util.Preconditions;
import com.owl.kafka.util.Constants;
import com.owl.kafka.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class ClientConfigs extends ConfigLoader{

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientConfigs.class);

    static final String CLIENT_TOPIC = "client.topic";

    static final String CLIENT_WORKER_NUM = "client.worker.num";

    static final  String CLIENT_CONFIG_FILE = "push_client.properties";

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

}
