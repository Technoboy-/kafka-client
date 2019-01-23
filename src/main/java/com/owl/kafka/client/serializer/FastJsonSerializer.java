package com.owl.kafka.client.serializer;

import com.alibaba.fastjson.JSON;
import com.owl.kafka.client.util.Constants;


/**
 * @author Tboy
 */
public class FastJsonSerializer<T> implements Serializer<T>{

	@Override
	public byte[] serialize(T obj)  {
		String json = JSON.toJSONString(obj);
		return json.getBytes(Constants.UTF8);
	}

	@Override
	public T deserialize(byte[] src, Class<T> clazz) {
		return JSON.parseObject(new String(src, Constants.UTF8), clazz);
	}

}
