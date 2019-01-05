package com.tt.kafka.consumer.service;

import com.tt.kafka.consumer.DefaultKafkaConsumerImpl;
import com.tt.kafka.consumer.Record;
import com.tt.kafka.consumer.listener.AcknowledgeMessageListener;
import com.tt.kafka.consumer.listener.MessageListener;
import com.tt.kafka.metric.MonitorImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: jiwei.guo
 * @Date: 2019/1/5 5:50 PM
 */
public class ProxyAcknowledgeMessageListenerService<K, V> extends ProxyCommonMessageListenerService<K, V>{

    private static final Logger LOG = LoggerFactory.getLogger(ProxyAcknowledgeMessageListenerService.class);

    private final AcknowledgeMessageListener<K, V> messageListener;

    private ProxyAcknowledgment proxyAcknowledgment;

    public ProxyAcknowledgeMessageListenerService(DefaultKafkaConsumerImpl<K, V> consumer, MessageListener<K, V> messageListener) {
        super(consumer);
        this.messageListener = (AcknowledgeMessageListener)messageListener;
        MonitorImpl.getDefault().recordConsumeHandlerCount(1);
    }

    public void setProxyAcknowledgment(ProxyAcknowledgment proxyAcknowledgment){
        this.proxyAcknowledgment = proxyAcknowledgment;
    }

    @Override
    public void onMessage(ConsumerRecord<byte[], byte[]> record) {
        //
    }

    public void onMessage(long msgId, ConsumerRecord<byte[], byte[]> record) {
        long now = System.currentTimeMillis();
        try {
            final Record<K, V> r = consumer.toRecord(record);
            r.setMsgId(msgId);
            messageListener.onMessage(r, new AcknowledgeMessageListener.Acknowledgment() {
                @Override
                public void acknowledge() {
                    proxyAcknowledgment.onAcknowledge(r);
                }
            });
        } catch (Throwable ex) {
            MonitorImpl.getDefault().recordConsumeProcessErrorCount(1);
            LOG.error("onMessage error", ex);
        } finally {
            MonitorImpl.getDefault().recordConsumeProcessCount(1);
            MonitorImpl.getDefault().recordConsumeProcessTime(System.currentTimeMillis() - now);
        }
    }

    public interface ProxyAcknowledgment<K, V>{

        void onAcknowledge(Record<K, V> record);
    }

    @Override
    public void close() {

    }
}
