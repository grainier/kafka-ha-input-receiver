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
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterListener;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerKafkaAdaptor {
    private final Properties firstConsumerProps;
    private final Properties secondConsumerProps;
    private final String topic;
    private ExecutorService executor;
    private int tenantId;
    private String partition;
    private String receiverName;
    private KafkaConsumerThread kafkaConsumerThread;
    private Log log = LogFactory.getLog(ConsumerKafkaAdaptor.class);

    public ConsumerKafkaAdaptor(String inTopic, String partition, int tenantId,
                                Properties firstConsumerProps, Properties secondConsumerProps,
                                String receiverName) {
        this.firstConsumerProps = firstConsumerProps;
        this.secondConsumerProps = secondConsumerProps;
        this.topic = inTopic;
        this.partition = partition;
        this.tenantId = tenantId;
        this.receiverName = receiverName;
    }

    public synchronized void shutdown() {
        if (kafkaConsumerThread != null) {
            kafkaConsumerThread.getFirstConsumer().close();
            kafkaConsumerThread.getSecondConsumer().close();
        }

        if (executor != null) {
            executor.shutdown();
        }
    }

    public void run(int numThreads, InputEventAdapterListener brokerListener) {
        try {
            executor = Executors.newFixedThreadPool(numThreads);
            this.kafkaConsumerThread = new KafkaConsumerThread(brokerListener, tenantId,
                    topic, partition, firstConsumerProps, secondConsumerProps, receiverName);
            executor.submit(kafkaConsumerThread);
            log.info("Kafka Consumers started listening on topic: " + topic);
        } catch (Throwable t) {
            log.error("Error while creating KafkaConsumer ", t);
        }
    }
}
