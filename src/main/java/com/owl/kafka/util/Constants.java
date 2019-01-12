package com.owl.kafka.util;

import java.nio.charset.Charset;

/**
 * @Author: Tboy
 */
public interface Constants {

    Charset UTF8 = Charset.forName("UTF-8");

    int CPU_SIZE = Runtime.getRuntime().availableProcessors();



}
