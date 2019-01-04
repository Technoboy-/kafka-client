package com.tt.kafka.client.transport.handler;

import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.client.transport.protocol.Header;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.client.transport.Connection;
import com.tt.kafka.serializer.SerializerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class PushHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushHandler.class);

    private final MessageListenerService messageListenerService;

    public PushHandler(MessageListenerService messageListenerService){
        this.messageListenerService = messageListenerService;
    }

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        Header header = (Header)SerializerImpl.getFastJsonSerializer().deserialize(packet.getHeader(), Header.class);
        ConsumerRecord record = new ConsumerRecord(header.getTopic(), header.getPartition(), header.getOffset(), packet.getKey(), packet.getValue());
        messageListenerService.onMessage(record);
    }

    class DecodeTask implements Runnable{

        private Packet packet;

        public DecodeTask(Packet packet){
            this.packet = packet;
        }

        @Override
        public void run() {
            Header header = (Header)SerializerImpl.getFastJsonSerializer().deserialize(packet.getHeader(), Header.class);
            ConsumerRecord record = new ConsumerRecord(header.getTopic(), header.getPartition(), header.getOffset(), packet.getKey(), packet.getValue());
            messageListenerService.onMessage(record);
        }
    }
}
