package com.tt.kafka.serializer;


import com.tt.kafka.serializer.extension.SPILoader;

/**
 * @Author: Tboy
 */
public class SerializerImpl {
	
	public static Serializer serializerImpl(){
		return SPILoader.getSPIClass(Serializer.class).getExtension();
	}

	public static Serializer getSerializaer(String name){
		return SPILoader.getSPIClass(Serializer.class).getExtension(name);
	}

	public static Serializer getByteArraySerializaer(){
		return SPILoader.getSPIClass(Serializer.class).getExtension("bytearray");
	}

	public static Serializer getHessianSerializaer(){
		return SPILoader.getSPIClass(Serializer.class).getExtension("hessian");
	}

	public static Serializer getJacksonSerializaer(){
		return SPILoader.getSPIClass(Serializer.class).getExtension("jackson");
	}

	public static Serializer getStringSerializaer(){
		return SPILoader.getSPIClass(Serializer.class).getExtension("string");
	}
}
