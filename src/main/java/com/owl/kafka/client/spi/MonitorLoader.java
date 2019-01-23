package com.owl.kafka.client.spi;

import com.owl.kafka.client.metric.Monitor;

import java.lang.reflect.Constructor;


/**
 * @Author: Tboy
 */
public class MonitorLoader extends SPILoader<Monitor> {

    protected final Class<Monitor> type;

    protected final Class<MonitorConfig> spi;

    private MonitorLoader(Class<Monitor> clazz, Class<MonitorConfig> spi){
        this.type = clazz;
        this.spi = spi;
        loadExtensionClasses(clazz);
    }

    public Monitor getExtension() {
        return this.getExtension(this.type.getAnnotation(spi).value());
    }

    public static MonitorLoader getSPIClass(Class<Monitor> clazz, Class<MonitorConfig> spi){
        SPILoader spiLoader = EXTENSION_LOADERS.get(clazz);
        if(spiLoader == null){
            spiLoader = new MonitorLoader(clazz, spi);
            EXTENSION_LOADERS.putIfAbsent(clazz, spiLoader);
        }
        return (MonitorLoader)spiLoader;
    }

    protected Monitor newInstance(Class<?> type) {
        try {
            Constructor constructor = type.getConstructor(String.class, boolean.class);
            constructor.setAccessible(true);
            return (Monitor) constructor.newInstance(this.type.getAnnotation(spi).name(), this.type.getAnnotation(spi).enable());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
