/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.event.input.adapter.kafka.ha;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterListener;
import org.wso2.siddhi.core.SiddhiEventOffsetHolder;

import java.util.Collections;
import java.util.Properties;


public class KafkaConsumerThread implements Runnable {
    private KafkaConsumer<byte[], byte[]> firstConsumer = null;
    private KafkaConsumer<byte[], byte[]> secondConsumer = null;
    private InputEventAdapterListener brokerListener;
    private int tenantId;
    private String receiverName;
    private Log log = LogFactory.getLog(KafkaConsumerThread.class);

    public KafkaConsumerThread(InputEventAdapterListener inBrokerListener, int tenantId, String topic,
                               String partition, Properties firstConsumerProps, Properties secondConsumerProps,
                               String receiverName) {
        try {
            this.brokerListener = inBrokerListener;
            this.tenantId = tenantId;
            this.receiverName = receiverName;
            TopicPartition topicPartition = new TopicPartition(topic, Integer.parseInt(partition));

            // TODO: seek logic
            this.firstConsumer = new KafkaConsumer<>(firstConsumerProps);
            firstConsumer.assign(Collections.singletonList(topicPartition));
            Long lastOffset = SiddhiEventOffsetHolder.getLastEventOffset(receiverName);
            if (lastOffset != null) {
                firstConsumer.seek(topicPartition, lastOffset);
            }

            this.secondConsumer = new KafkaConsumer<>(secondConsumerProps);
            secondConsumer.assign(Collections.singletonList(topicPartition));
            lastOffset = SiddhiEventOffsetHolder.getLastEventOffset(receiverName);
            if (lastOffset != null) {
                secondConsumer.seek(topicPartition, lastOffset);
            }
        } catch (Throwable t) {
            log.error(t);
        }
    }

    public void run() {
        log.info("Kafka listening thread started.");
        while (true) {
            try {
                PrivilegedCarbonContext.startTenantFlow();
                PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantId(tenantId);

                ConsumerRecords<byte[], byte[]> firstConsumerRecords = firstConsumer.poll(200);
                for (ConsumerRecord record : firstConsumerRecords) {
                    String event = record.value().toString();
                    SiddhiEventOffsetHolder.putEventOffset(receiverName, record.offset());
                    log.info("Consumer 1 Offset : " + record.offset());
                    if (log.isDebugEnabled()) {
                        log.debug("Event received in Kafka Event Adaptor: " + event + ", offSet: " +
                                record.offset() + ", key: " + record.key() + ", topicPartition: " + record.partition());
                    }
                    brokerListener.onEvent(event);
                }

                ConsumerRecords<byte[], byte[]> secondConsumerRecords = secondConsumer.poll(200);
                for (ConsumerRecord record : secondConsumerRecords) {
                    String event = record.value().toString();
                    SiddhiEventOffsetHolder.putEventOffset(receiverName, record.offset());
                    log.info("Consumer 2 Offset : " + record.offset());
                    if (log.isDebugEnabled()) {
                        log.debug("Event received in Kafka Event Adaptor: " + event + ", offSet: " +
                                record.offset() + ", key: " + record.key() + ", topicPartition: " + record.partition());
                    }
                    brokerListener.onEvent(event);
                }
            } catch (Throwable t) {
                log.error("Error while consuming event " + t);
            } finally {
                PrivilegedCarbonContext.endTenantFlow();
            }
        }
    }

    public KafkaConsumer<byte[], byte[]> getFirstConsumer() {
        return firstConsumer;
    }

    public KafkaConsumer<byte[], byte[]> getSecondConsumer() {
        return secondConsumer;
    }
}
