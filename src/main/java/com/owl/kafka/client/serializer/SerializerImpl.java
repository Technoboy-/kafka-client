package com.owl.kafka.client.serializer;


import com.owl.kafka.client.spi.SPI;
import com.owl.kafka.client.spi.SerializerLoader;

/**
 * @Author: Tboy
 */
public class SerializerImpl {
	
	public static Serializer serializerImpl(){
		return SerializerLoader.getSPIClass(Serializer.class, SPI.class).getExtension();
	}

	public static Serializer getSerializer(String name){
		return SerializerLoader.getSPIClass(Serializer.class, SPI.class).getExtension(name);
	}

	public static Serializer getByteArraySerializer(){
		return SerializerLoader.getSPIClass(Serializer.class, SPI.class).getExtension("bytearray");
	}

	public static Serializer getHessianSerializer(){
		return SerializerLoader.getSPIClass(Serializer.class, SPI.class).getExtension("hessian");
	}

	public static Serializer getJacksonSerializer(){
		return SerializerLoader.getSPIClass(Serializer.class, SPI.class).getExtension("jackson");
	}

	public static Serializer getFastJsonSerializer(){
		return SerializerLoader.getSPIClass(Serializer.class, SPI.class).getExtension("fastjson");
	}

	public static Serializer getStringSerializer(){
		return SerializerLoader.getSPIClass(Serializer.class, SPI.class).getExtension("string");
	}

	public static Serializer getProtoStuffSerializer(){
		return SerializerLoader.getSPIClass(Serializer.class, SPI.class).getExtension("protostuff");
	}
}
