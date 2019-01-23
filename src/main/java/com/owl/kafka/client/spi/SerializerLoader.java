package com.owl.kafka.client.spi;

import com.owl.kafka.client.serializer.Serializer;


/**
 * @Author: Tboy
 */
public class SerializerLoader extends SPILoader<Serializer> {

    protected final Class<Serializer> type;

    protected final Class<SPI> spi;

    private SerializerLoader(Class<Serializer> clazz, Class<SPI> spi){
        this.type = clazz;
        this.spi = spi;
        loadExtensionClasses(clazz);
    }

    public Serializer getExtension() {
        return this.getExtension(this.type.getAnnotation(spi).value());
    }

    public static SerializerLoader getSPIClass(Class<Serializer> clazz, Class<SPI> spi){
        SPILoader spiLoader = EXTENSION_LOADERS.get(clazz);
        if(spiLoader == null){
            spiLoader = new SerializerLoader(clazz, spi);
            EXTENSION_LOADERS.putIfAbsent(clazz, spiLoader);
        }
        return (SerializerLoader)spiLoader;
    }

    @Override
    protected Serializer newInstance(Class<?> type) {
        try {
            return (Serializer) type.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
