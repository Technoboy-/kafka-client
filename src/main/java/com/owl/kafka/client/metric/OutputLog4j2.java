package com.owl.kafka.client.metric;



import org.apache.log4j.spi.LoggerFactory;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.IOException;

/**
 * @Author: Tboy
 */
public class OutputLog4j2 {

    private  Logger logger;

//    private final RollingFileAppender appender;
//
//    private final String name;

    public OutputLog4j2(String name){
//        this.name = name;
//        try {
//            PatternLayout patternLayout = PatternLayout.createDefaultLayout();
//            this.appender = RollingFileAppender
//                    .newBuilder()
//                    .withLayout(patternLayout)
//                    .withName(this.name)
//                    .withAppend(true).withBufferedIo(true)
//                    .build();
////            this.logger = new Logger(new LoggerContext("monitor"), "monitor", SimpleMessageFactory.INSTANCE);
//            this.logger = LoggerFactory.class
//            this.logger.addAppender(this.appender);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    public void info(String message){
        this.logger.info(message);
    }

}
