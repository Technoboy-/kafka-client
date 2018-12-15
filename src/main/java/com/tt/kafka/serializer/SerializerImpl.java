package com.tt.kafka.serializer;


import com.tt.kafka.serializer.extension.SPILoader;

/**
 * @Author: Tboy
 */
public class SerializerImpl {
	
	public static Serializer serializerImpl(){
		return SPILoader.getSPIClass(Serializer.class).getExtension();
	}

	public static Serializer getSerializer(String name){
		return SPILoader.getSPIClass(Serializer.class).getExtension(name);
	}

	public static Serializer getByteArraySerializer(){
		return SPILoader.getSPIClass(Serializer.class).getExtension("bytearray");
	}

	public static Serializer getHessianSerializer(){
		return SPILoader.getSPIClass(Serializer.class).getExtension("hessian");
	}

	public static Serializer getJacksonSerializer(){
		return SPILoader.getSPIClass(Serializer.class).getExtension("jackson");
	}

	public static Serializer getStringSerializer(){
		return SPILoader.getSPIClass(Serializer.class).getExtension("string");
	}
}
