package com.owl.kafka.client.metric;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.io.IOException;

/**
 * @Author: Tboy
 */
public class OutputLog {

    private final Logger logger;

    private final RollingFileAppender appender;

    private final String name;

    public OutputLog(String name){
        this.name = name;
        try {
            this.appender = new RollingFileAppender(new PatternLayout("%m%n"), this.name,  true);
            this.appender.setBufferedIO(false);
            this.appender.setMaxFileSize("10MB");
            this.appender.setMaxBackupIndex(1);
            this.logger = Logger.getLogger("monitor");
            this.logger.removeAllAppenders();
            this.logger.addAppender(this.appender);
            this.logger.setAdditivity(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void info(String message){
        this.logger.info(message);
    }

}
