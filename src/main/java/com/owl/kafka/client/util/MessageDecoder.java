package com.owl.kafka.client.util;

import com.owl.kafka.client.transport.message.Message;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.serializer.SerializerImpl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class MessageDecoder {

    public static Message decode(byte[] body){
        Message message = new Message();

        ByteBuffer buffer = ByteBuffer.wrap(body);
        //
        int headerLength = buffer.getInt();
        byte[] headerInBytes = new byte[headerLength];
        buffer.get(headerInBytes, 0, headerLength);
        Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, Header.class);
        message.setHeader(header);
        message.setHeaderInBytes(headerInBytes);
        //
        int keyLength = buffer.getInt();
        byte[] key = new byte[keyLength];
        buffer.get(key, 0, keyLength);
        message.setKey(key);
        //
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value, 0, valueLength);
        message.setValue(value);

        return message;
    }

    public static List<Message> decodes(byte[] body){
        List<Message> messages = new ArrayList<>();
        //
        ByteBuffer buffer = ByteBuffer.wrap(body);
        while(buffer.hasRemaining()){
            Message message = new Message();
            //
            int headerLength = buffer.getInt();
            byte[] headerInBytes = new byte[headerLength];
            buffer.get(headerInBytes, 0, headerLength);
            Header header = (Header) SerializerImpl.getFastJsonSerializer().deserialize(headerInBytes, Header.class);
            message.setHeader(header);
            message.setHeaderInBytes(headerInBytes);
            //
            int keyLength = buffer.getInt();
            byte[] key = new byte[keyLength];
            buffer.get(key, 0, keyLength);
            message.setKey(key);
            //
            int valueLength = buffer.getInt();
            byte[] value = new byte[valueLength];
            buffer.get(value, 0, valueLength);
            message.setValue(value);
            //
            messages.add(message);
        }
        return messages;
    }
}
