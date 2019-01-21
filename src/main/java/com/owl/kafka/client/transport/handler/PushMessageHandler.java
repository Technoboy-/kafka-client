package com.owl.kafka.client.transport.handler;

import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.transport.protocol.Header;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.client.util.Packets;
import com.owl.kafka.consumer.Record;
import com.owl.kafka.consumer.service.MessageListenerService;
import com.owl.kafka.consumer.service.PushAcknowledgeMessageListenerService;
import com.owl.kafka.serializer.SerializerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class PushMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushMessageHandler.class);

    private final PushAcknowledgeMessageListenerService messageListenerService;

    public PushMessageHandler(MessageListenerService messageListenerService){
        this.messageListenerService = (PushAcknowledgeMessageListenerService)messageListenerService;
    }

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        LOGGER.debug("received push message : {} ", packet);
        Header header = (Header)SerializerImpl.getFastJsonSerializer().deserialize(packet.getHeader(), Header.class);
        ConsumerRecord record = new ConsumerRecord(header.getTopic(), header.getPartition(), header.getOffset(), packet.getKey(), packet.getValue());
        messageListenerService.onMessage(header.getMsgId(), record, new PushAcknowledgeMessageListenerService.AcknowledgmentCallback() {
            @Override
            public void onAcknowledge(Record record) {
                try {
                    connection.send(Packets.ack(record.getMsgId()));
                } catch (ChannelInactiveException ex) {
                    //in this case, we do not need to care about it, because push server has repush policy
                    LOGGER.error("ChannelInactiveException, closing the channel ", ex);
                    connection.close();
                }
            }
        });
    }

}
