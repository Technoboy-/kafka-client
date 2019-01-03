package com.tt.kafka.util;

import java.nio.charset.Charset;

/**
 * @Author: Tboy
 */
public interface Constants {

    Charset UTF8 = Charset.forName("UTF-8");

    int CPU_SIZE = Runtime.getRuntime().availableProcessors();

    //
    String PUSH_CLIENT_TOPIC = "push.client.topic";

    //
    String ZOOKEEPER_SERVER_LIST = "zookeeper.server.list";

    String ZOOKEEPER_NAMESPACE = "push_server";

    String ZOOKEEPER_PROVIDERS = "/%s/providers";

    String ZOOKEEPER_CONSUMERS = "/%s/consumers";

    String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";

    String ZOOKEEPER_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms";

    String ZOOKEEPER_TOPIC = "zookeeper.topic";

    //
    String PUSH_SERVER_PORT = "push.server.port";

    String PUSH_SERVER_BOSS_NUM = "push.server.boss.num";

    String PUSH_SERVER_WORKER_NUM = "push.server.worker.num";

    String PUSH_SERVER_QUEUE_SIZE = "push.server.queue.size";

    String PUSH_SERVER_GROUP_ID = "push.server.group.id";

    String PUSH_SERVER_KAFKA_SERVER_LIST = "push.server.kafka.server.list";

}
