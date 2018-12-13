package com.tt.kafka.serializer;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * @Author: Tboy
 */
public class HessianSerializer<T> implements Serializer<T>{

    @Override
    public byte[] serialize(T obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HessianOutput hessianOutput = new HessianOutput(bos);
            hessianOutput.writeObject(obj);
            return bos.toByteArray();
        } catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }

    @Override
    public T deserialize(byte[] src, Class<T> clazz) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(src);
            HessianInput hessianInput = new HessianInput(bis);
            return (T)hessianInput.readObject();
        } catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }
}
