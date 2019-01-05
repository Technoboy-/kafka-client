package com.tt.kafka.client.transport.handler;

import com.tt.kafka.client.transport.Connection;
import com.tt.kafka.client.transport.protocol.Command;
import com.tt.kafka.client.transport.protocol.Header;
import com.tt.kafka.client.transport.protocol.Packet;
import com.tt.kafka.consumer.Record;
import com.tt.kafka.consumer.TopicPartition;
import com.tt.kafka.consumer.listener.AcknowledgeMessageListener;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.consumer.service.MessageListenerService;
import com.tt.kafka.consumer.service.ProxyAcknowledgeMessageListenerService;
import com.tt.kafka.consumer.service.ProxyBatchAcknowledgeMessageListenerService;
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

    private final MessageListenerService messageListenerService;

    public PushMessageHandler(MessageListener messageListener, MessageListenerService messageListenerService){
        this.messageListenerService = messageListenerService;
    }

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        LOGGER.debug("received msgId : {} ", packet.getMsgId());
        Header header = (Header)SerializerImpl.getFastJsonSerializer().deserialize(packet.getHeader(), Header.class);
        ConsumerRecord record = new ConsumerRecord(header.getTopic(), header.getPartition(), header.getOffset(), packet.getKey(), packet.getValue());
        if(messageListenerService instanceof ProxyAcknowledgeMessageListenerService){
            ProxyAcknowledgeMessageListenerService service = (ProxyAcknowledgeMessageListenerService)messageListenerService;
            service.setProxyAcknowledgment(new ProxyAcknowledgeMessageListenerService.ProxyAcknowledgment() {
                @Override
                public void onAcknowledge(Record record) {
                    connection.send(ackPacket(record.getMsgId()));
                }
            });
            service.onMessage(packet.getMsgId(), record);

        } else if(messageListenerService instanceof ProxyBatchAcknowledgeMessageListenerService) {
            ProxyBatchAcknowledgeMessageListenerService service = (ProxyBatchAcknowledgeMessageListenerService)messageListenerService;
            service.setProxyAcknowledgment(new ProxyBatchAcknowledgeMessageListenerService.ProxyAcknowledgment() {
                @Override
                public void onAcknowledge(List list) {
                    List<Record> records = (List<Record>)list;
                    List<Record> highestOffsetRecordList = getHighestOffsetList(records);
                    for(Record r : highestOffsetRecordList){
                        connection.send(ackPacket(r.getMsgId()));
                    }
                }
            });
            service.onMessage(packet.getMsgId(), record);

        } else {
            messageListenerService.onMessage(record);
        }
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
