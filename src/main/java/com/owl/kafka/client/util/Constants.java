package com.owl.kafka.client.util;

import java.nio.charset.Charset;

/**
 * @Author: Tboy
 */
public interface Constants {

    Charset UTF8 = Charset.forName("UTF-8");

    int CPU_SIZE = Runtime.getRuntime().availableProcessors();

    int K_BYTES = 1024;

    int M_BYTES = K_BYTES * 1024;

    String PROXY_MODEL = "proxy.model";

}
