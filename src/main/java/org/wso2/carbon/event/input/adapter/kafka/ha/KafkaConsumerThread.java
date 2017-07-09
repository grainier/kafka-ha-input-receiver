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
import org.wso2.siddhi.core.SnapshotableElementsHolder;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class KafkaConsumerThread implements Runnable {
    private static final Pattern EVENT_PATTERN = Pattern.compile("^(.*)_(\\d+)_(\\d+)_(.*)$");
    private KafkaConsumer<byte[], byte[]> consumer = null;
    private String queueId;
    private BlockingQueue<Event> recordQueue;
    private Log log = LogFactory.getLog(KafkaConsumerThread.class);

    public KafkaConsumerThread(String receiverName, String topic, String partition, Properties consumerProps,
                               String queueId, BlockingQueue<Event> recordQueue) {
        try {
            this.recordQueue = recordQueue;
            this.queueId = queueId;
            TopicPartition topicPartition = new TopicPartition(topic, Integer.parseInt(partition));
            this.consumer = new KafkaConsumer<>(consumerProps);
            consumer.assign(Collections.singletonList(topicPartition));

            Map<String, Object> existingState = SnapshotableElementsHolder.getState(receiverName);
            if (existingState != null) {
                Object existingOffset = existingState.get(queueId);
                if (existingOffset != null) {
                    consumer.seek(topicPartition, (Long) existingOffset);
                }
            }
        } catch (Throwable t) {
            log.error(t);
        }
    }

    public void run() {
        log.info("Kafka listening thread started.");
        while (true) {
            try {
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(200);
                for (ConsumerRecord record : consumerRecords) {
                    if (log.isDebugEnabled()) {
                        log.debug("Event received in Kafka Event Adaptor (Consumer 1): " + record.value() +
                                ", offSet: " + record.offset() + ", key: " + record.key() + ", topicPartition: " +
                                record.partition());
                    }
                    recordQueue.add(new Event(record.value().toString(), queueId, record.offset()));
                }
            } catch (Throwable t) {
                log.error("Error while consuming event " + t);
            }
        }
    }

    public KafkaConsumer<byte[], byte[]> getConsumer() {
        return consumer;
    }

    class Event implements Comparable<Event> {
        private String publisher;
        private Long timestamp;
        private Long sequence;
        private String queueId;
        private String event;
        private long offset;

        Event(String eventString, String queueId, long offset) {
            Matcher matcher = EVENT_PATTERN.matcher(eventString);
            while (matcher.find()) {
                publisher = matcher.group(1);
                timestamp = Long.valueOf(matcher.group(2));
                sequence = Long.valueOf(matcher.group(3));
                event = matcher.group(4);
            }
            this.queueId = queueId;
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

        public String getQueueId() {
            return queueId;
        }

        @Override
        public int compareTo(Event o) {
            return (Long.compare(this.timestamp, o.getTimestamp()) != 0) ?
                    Long.compare(this.timestamp, o.getTimestamp()) :
                    Long.compare(this.sequence, o.getSequence());
        }
    }
}
