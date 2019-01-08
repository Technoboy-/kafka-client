package com.tt.kafka.client.transport.handler;

import com.tt.kafka.client.transport.Connection;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.client.transport.protocol.Header;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.consumer.Record;
import com.tt.kafka.consumer.TopicPartition;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.consumer.service.ProxyAcknowledgeMessageListenerService;
import com.tt.kafka.serializer.SerializerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Tboy
 */
public class PushMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushMessageHandler.class);

    private final ProxyAcknowledgeMessageListenerService messageListenerService;

    public PushMessageHandler(MessageListenerService messageListenerService){
        this.messageListenerService = (ProxyAcknowledgeMessageListenerService)messageListenerService;
    }

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        LOGGER.debug("received msgId : {} ", packet.getMsgId());
        Header header = (Header)SerializerImpl.getFastJsonSerializer().deserialize(packet.getHeader(), Header.class);
        ConsumerRecord record = new ConsumerRecord(header.getTopic(), header.getPartition(), header.getOffset(), packet.getKey(), packet.getValue());
        messageListenerService.onMessage(packet.getMsgId(), record, new ProxyAcknowledgeMessageListenerService.AcknowledgmentCallback() {
            @Override
            public void onAcknowledge(Record record) {
                connection.send(ackPacket(record.getMsgId()));
            }
        });
    }

    private List<Record> getHighestOffsetList(List<Record> records){
        Map<TopicPartition, Record> highestOffsetMap = new HashMap<>(records.size());
        for(Record record : records){
            TopicPartition topicPartition = new TopicPartition(record.getTopic(), record.getPartition());
            Record existed = highestOffsetMap.get(topicPartition);
            if (existed == null || record.getOffset() > existed.getOffset()) {
                highestOffsetMap.put(topicPartition, record);
            }
        }
        return new ArrayList<>(highestOffsetMap.values());
    }

    private Packet ackPacket(long msgId){
        Packet ack = new Packet();
        ack.setCmd(Command.ACK.getCmd());
        ack.setMsgId(msgId);
        ack.setHeader(new byte[0]);
        ack.setKey(new byte[0]);
        ack.setValue(new byte[0]);
        return ack;
    }
}
