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
import org.wso2.carbon.event.input.adapter.kafka.ha.internal.util.KafkaEventAdapterConstants;
import org.wso2.siddhi.core.SiddhiEventOffsetHolder;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class KafkaConsumerThread implements Runnable {
    private static final Pattern EVENT_PATTERN = Pattern.compile("^(.*)_(\\d+)_(\\d+)_(.*)$");
    private KafkaConsumer<byte[], byte[]> firstConsumer = null;
    private KafkaConsumer<byte[], byte[]> secondConsumer = null;
    private InputEventAdapterListener brokerListener;
    private int tenantId;
    private String receiverName;
    private Log log = LogFactory.getLog(KafkaConsumerThread.class);
    private Queue<Event> firstEventQueue = new LinkedList<>();
    private Queue<Event> secondEventQueue = new LinkedList<>();
    private Event lastProcessedEvent = null;
    private ExecutorService executor;

    public KafkaConsumerThread(InputEventAdapterListener inBrokerListener, int tenantId, String topic,
                               String partition, Properties firstConsumerProps, Properties secondConsumerProps,
                               String receiverName) {
        try {
            // TODO: better way of doing this?
            this.executor = Executors.newFixedThreadPool(2);
            this.brokerListener = inBrokerListener;
            this.tenantId = tenantId;
            this.receiverName = receiverName;
            TopicPartition topicPartition = new TopicPartition(topic, Integer.parseInt(partition));

            // TODO: seek logic?
            this.firstConsumer = new KafkaConsumer<>(firstConsumerProps);
            firstConsumer.assign(Collections.singletonList(topicPartition));

            this.secondConsumer = new KafkaConsumer<>(secondConsumerProps);
            secondConsumer.assign(Collections.singletonList(topicPartition));

            Map<String, Object> offsetInfo = SiddhiEventOffsetHolder.getLastEventOffset(receiverName);
            if (offsetInfo != null) {
                if (offsetInfo.get(KafkaEventAdapterConstants.FIRST_CONSUMER_OFFSET_KEY) != null)
                    firstConsumer.seek(topicPartition, (Long) offsetInfo.get(KafkaEventAdapterConstants.FIRST_CONSUMER_OFFSET_KEY));
                if (offsetInfo.get(KafkaEventAdapterConstants.SECOND_CONSUMER_OFFSET_KEY) != null)
                    secondConsumer.seek(topicPartition, (Long) offsetInfo.get(KafkaEventAdapterConstants.SECOND_CONSUMER_OFFSET_KEY));
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


                executor.execute(new Runnable() {
                    public void run() {
                        ConsumerRecords<byte[], byte[]> firstConsumerRecords = firstConsumer.poll(200);
                        for (ConsumerRecord record : firstConsumerRecords) {
                            if (log.isDebugEnabled()) {
                                log.debug("Event received in Kafka Event Adaptor (Consumer 1): " + record.value() + ", offSet: " +
                                        record.offset() + ", key: " + record.key() + ", topicPartition: " + record.partition());
                            }
                            firstEventQueue.add(new Event(record.value().toString(), record.offset()));
                        }
                    }
                });

                executor.execute(new Runnable() {
                    public void run() {
                        ConsumerRecords<byte[], byte[]> secondConsumerRecords = secondConsumer.poll(200);
                        for (ConsumerRecord record : secondConsumerRecords) {
                            if (log.isDebugEnabled()) {
                                log.debug("Event received in Kafka Event Adaptor (Consumer 2): " + record.value() + ", offSet: " +
                                        record.offset() + ", key: " + record.key() + ", topicPartition: " + record.partition());
                            }
                            secondEventQueue.add(new Event(record.value().toString(), record.offset()));
                        }
                    }
                });


                Event e1 = firstEventQueue.peek();
                Event e2 = secondEventQueue.peek();

                while (e1 != null || e2 != null) {

                    if (e1 != null && e2 != null) {
                        if (e1.compareTo(e2) == 0) {

                            if (lastProcessedEvent == null) {
                                brokerListener.onEvent(e1.getEvent());
                                lastProcessedEvent = e1;

                                updateOffsetMeta(KafkaEventAdapterConstants.FIRST_CONSUMER_OFFSET_KEY, firstEventQueue.remove().getOffset());
                                updateOffsetMeta(KafkaEventAdapterConstants.SECOND_CONSUMER_OFFSET_KEY, secondEventQueue.remove().getOffset());
                            } else {
                                if (e1.compareTo(lastProcessedEvent) <= 0) {
                                    // drop both
                                    firstEventQueue.remove();
                                    secondEventQueue.remove();

                                } else {
                                    brokerListener.onEvent(e1.getEvent());
                                    lastProcessedEvent = e1;

                                    updateOffsetMeta(KafkaEventAdapterConstants.FIRST_CONSUMER_OFFSET_KEY, firstEventQueue.remove().getOffset());
                                    updateOffsetMeta(KafkaEventAdapterConstants.SECOND_CONSUMER_OFFSET_KEY, secondEventQueue.remove().getOffset());
                                }
                            }

                            e1 = firstEventQueue.peek();
                            e2 = secondEventQueue.peek();

                        } else if (e1.compareTo(e2) < 0) {

                            if (lastProcessedEvent == null) {

                                brokerListener.onEvent(e1.getEvent());
                                lastProcessedEvent = e1;

                                updateOffsetMeta(KafkaEventAdapterConstants.FIRST_CONSUMER_OFFSET_KEY, firstEventQueue.remove().getOffset());

                            } else {

                                if (e1.compareTo(lastProcessedEvent) <= 0) {
                                    // drop event
                                    firstEventQueue.remove();

                                } else {
                                    brokerListener.onEvent(e1.getEvent());
                                    lastProcessedEvent = e1;

                                    updateOffsetMeta(KafkaEventAdapterConstants.FIRST_CONSUMER_OFFSET_KEY, firstEventQueue.remove().getOffset());
                                }

                            }
                            e1 = firstEventQueue.peek();

                        } else {
                            if (lastProcessedEvent == null) {

                                brokerListener.onEvent(e2.getEvent());
                                lastProcessedEvent = e2;

                                updateOffsetMeta(KafkaEventAdapterConstants.SECOND_CONSUMER_OFFSET_KEY, secondEventQueue.remove().getOffset());

                            } else {

                                if (e2.compareTo(lastProcessedEvent) <= 0) {
                                    // drop event
                                    secondEventQueue.remove();

                                } else {
                                    brokerListener.onEvent(e2.getEvent());
                                    lastProcessedEvent = e2;

                                    updateOffsetMeta(KafkaEventAdapterConstants.SECOND_CONSUMER_OFFSET_KEY, secondEventQueue.remove().getOffset());
                                }

                            }
                            e2 = secondEventQueue.peek();

                        }
                    } else if (e1 != null) {

                        // TODO : retry for e2

                        if (lastProcessedEvent == null) {

                            brokerListener.onEvent(e1.getEvent());
                            lastProcessedEvent = e1;

                            updateOffsetMeta(KafkaEventAdapterConstants.FIRST_CONSUMER_OFFSET_KEY, firstEventQueue.remove().getOffset());

                        } else {

                            if (e1.compareTo(lastProcessedEvent) <= 0) {
                                // drop event
                                firstEventQueue.remove();

                            } else {
                                brokerListener.onEvent(e1.getEvent());
                                lastProcessedEvent = e1;

                                updateOffsetMeta(KafkaEventAdapterConstants.FIRST_CONSUMER_OFFSET_KEY, firstEventQueue.remove().getOffset());
                            }

                        }
                        e1 = firstEventQueue.peek();

                    } else {

                        // TODO : retry for e1


                        if (lastProcessedEvent == null) {

                            brokerListener.onEvent(e2.getEvent());
                            lastProcessedEvent = e2;

                            updateOffsetMeta(KafkaEventAdapterConstants.SECOND_CONSUMER_OFFSET_KEY, secondEventQueue.remove().getOffset());

                        } else {

                            if (e2.compareTo(lastProcessedEvent) <= 0) {
                                // drop event
                                secondEventQueue.remove();

                            } else {
                                brokerListener.onEvent(e2.getEvent());
                                lastProcessedEvent = e2;

                                updateOffsetMeta(KafkaEventAdapterConstants.SECOND_CONSUMER_OFFSET_KEY, secondEventQueue.remove().getOffset());
                            }

                        }
                        e2 = secondEventQueue.peek();

                    }

                }


            } catch (Throwable t) {
                log.error("Error while consuming event " + t);
            } finally {
                PrivilegedCarbonContext.endTenantFlow();
            }
        }
    }

    private void updateOffsetMeta(String key, Object value) {
        Map<String, Object> offsetInfo = (SiddhiEventOffsetHolder.getLastEventOffset(receiverName) != null) ?
                SiddhiEventOffsetHolder.getLastEventOffset(receiverName) :
                new HashMap<String, Object>();
        offsetInfo.put(key, value);
        offsetInfo.put(KafkaEventAdapterConstants.LAST_EVENT_ID_KEY, lastProcessedEvent.getId());
        SiddhiEventOffsetHolder.putEventOffset(receiverName, offsetInfo);
    }

    public KafkaConsumer<byte[], byte[]> getFirstConsumer() {
        return firstConsumer;
    }

    public KafkaConsumer<byte[], byte[]> getSecondConsumer() {
        return secondConsumer;
    }

    class Event implements Comparable<Event> {
        private String publisher;
        private Long timestamp;
        private Long sequence;
        private String event;
        private long offset;

        Event(String eventString, long offset) {
            Matcher matcher = EVENT_PATTERN.matcher(eventString);
            while (matcher.find()) {
                publisher = matcher.group(1);
                timestamp = Long.valueOf(matcher.group(2));
                sequence = Long.valueOf(matcher.group(3));
                event = matcher.group(4);
            }
            this.offset = offset;
        }

        public String getPublisher() {
            return publisher;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public Long getSequence() {
            return sequence;
        }

        public String getEvent() {
            return event;
        }

        public long getOffset() {
            return offset;
        }

        public String getId() {
            return timestamp + "_" + sequence;
        }

        @Override
        public int compareTo(Event o) {
            return (Long.compare(this.timestamp, o.getTimestamp()) != 0) ?
                    Long.compare(this.timestamp, o.getTimestamp()) :
                    Long.compare(this.sequence, o.getSequence());
        }

    }
}
