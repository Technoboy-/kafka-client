package com.owl.kafka.client.transport.handler;

import com.owl.kafka.client.service.InvokerPromise;
import com.owl.kafka.client.transport.Connection;
import com.owl.kafka.client.transport.protocol.Packet;
import com.owl.kafka.consumer.service.MessageListenerService;
import com.owl.kafka.consumer.service.PullAcknowledgeMessageListenerService;
import com.owl.kafka.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
public class PullMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullMessageHandler.class);

    private final PullAcknowledgeMessageListenerService messageListenerService;

    public PullMessageHandler(MessageListenerService messageListenerService){
        this.messageListenerService = (PullAcknowledgeMessageListenerService)messageListenerService;
    }

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received pull message: {}, from : {}", packet, NetUtils.getRemoteAddress(connection.getChannel()));
        }
        InvokerPromise invokerPromise = InvokerPromise.get(packet.getMsgId());
        if(invokerPromise != null){
            if(invokerPromise.getInvokeCallback() != null){
                invokerPromise.executeInvokeCallback();
            } else{
                invokerPromise.doReceive(packet);
            }
        }
        if(packet.getValue().length >= 1){
            messageListenerService.onMessage(connection, packet);
        }
    }

}
