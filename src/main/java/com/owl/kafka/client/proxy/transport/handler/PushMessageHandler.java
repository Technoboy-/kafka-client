package com.owl.kafka.client.proxy.transport.handler;

import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.exceptions.ChannelInactiveException;
import com.owl.kafka.client.proxy.transport.message.Message;
import com.owl.kafka.client.proxy.transport.message.Header;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.util.MessageCodec;
import com.owl.kafka.client.proxy.util.Packets;
import com.owl.kafka.client.consumer.Record;
import com.owl.kafka.client.consumer.service.MessageListenerService;
import com.owl.kafka.client.consumer.service.PushAcknowledgeMessageListenerService;
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
        Message message = MessageCodec.decode(packet.getBody());
        Header header = message.getHeader();
        ConsumerRecord record = new ConsumerRecord(header.getTopic(), header.getPartition(), header.getOffset(), message.getKey(), message.getValue());
        messageListenerService.onMessage(header.getMsgId(), record, new PushAcknowledgeMessageListenerService.AcknowledgmentCallback() {
            @Override
            public void onAcknowledge(Record record) {
                try {
                    connection.send(Packets.ackPushReq(record.getMsgId()));
                } catch (ChannelInactiveException ex) {
                    //in this case, we do not need to care about it, because push server has repush policy
                    LOGGER.error("ChannelInactiveException, closing the channel ", ex);
                    connection.close();
                }
            }
        });
    }

}
