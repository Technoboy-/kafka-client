package com.tt.kafka.metric;

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

    private final String path;

    private final String name;

    public OutputLog(String path, String name){
        this.path = path.endsWith("/") ? path : path + "/";
        this.name = name.endsWith(".log") ? name : name + ".log";

        try {
            this.appender = new RollingFileAppender(new PatternLayout("%m%n"), this.path + this.name,  true);
            this.appender.setBufferedIO(false);
            this.appender.setMaxFileSize("10MB");
            this.appender.setMaxBackupIndex(1);
            this.logger = Logger.getLogger(name);
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
