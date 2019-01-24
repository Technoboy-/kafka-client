package com.owl.kafka.client.proxy.transport.handler;

import com.owl.kafka.client.consumer.service.MessageListenerService;
import com.owl.kafka.client.consumer.service.PullAcknowledgeMessageListenerService;
import com.owl.kafka.client.proxy.service.InvokerPromise;
import com.owl.kafka.client.proxy.transport.Connection;
import com.owl.kafka.client.proxy.transport.message.Message;
import com.owl.kafka.client.proxy.transport.protocol.Packet;
import com.owl.kafka.client.proxy.util.MessageCodec;
import com.owl.kafka.client.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Author: Tboy
 */
public class PullRespMessageHandler extends CommonMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PullRespMessageHandler.class);

    private final PullAcknowledgeMessageListenerService messageListenerService;

    public PullRespMessageHandler(MessageListenerService messageListenerService){
        this.messageListenerService = (PullAcknowledgeMessageListenerService)messageListenerService;
    }

    @Override
    public void handle(Connection connection, Packet packet) throws Exception {
        if(LOGGER.isDebugEnabled()){
            LOGGER.debug("received pull message: {}, from : {}", packet, NetUtils.getRemoteAddress(connection.getChannel()));
        }
        InvokerPromise invokerPromise = InvokerPromise.get(packet.getOpaque());
        if(invokerPromise != null){
            InvokerPromise.receive(packet);
            if(invokerPromise.getInvokeCallback() != null){
                invokerPromise.executeInvokeCallback();
            }
        }
        List<Message> messages = MessageCodec.decodes(packet.getBody());
        this.messageListenerService.onMessage(connection, messages);
    }


}
