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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerKafkaAdaptor {
    private final Properties props;
    private final String topic;
    private ExecutorService executor;
    private int tenantId;
    private String partitionList;
    private String receiverName;
    private List<KafkaConsumerThread> kafkaConsumerThreadList = new ArrayList<>();
    private Log log = LogFactory.getLog(ConsumerKafkaAdaptor.class);

    public ConsumerKafkaAdaptor(String inTopic, String partitionList, int tenantId,
                                Properties props, String receiverName) {
        this.props = props;
        this.topic = inTopic;
        this.partitionList = partitionList;
        this.tenantId = tenantId;
        this.receiverName = receiverName;
    }

    public synchronized void shutdown() {

        for(KafkaConsumerThread kafkaConsumerThread : kafkaConsumerThreadList){
            kafkaConsumerThread.getConsumer().close();
        }

        if (executor != null) {
            executor.shutdown();
        }
    }

    public void run(int numThreads, InputEventAdapterListener brokerListener) {
        try {
            // now launch all the threads
            executor = Executors.newFixedThreadPool(numThreads);

            // now create an object to consume the messages
            for (int i = 0; i < numThreads; i++) {
                KafkaConsumerThread kafkaConsumerThread = new KafkaConsumerThread
                        (brokerListener,tenantId, topic, partitionList, props, receiverName);
                kafkaConsumerThreadList.add(kafkaConsumerThread);
                executor.submit(kafkaConsumerThread);
            }
            log.info("Kafka Consumer started listening on topic: " + topic);
        } catch (Throwable t) {
            log.error("Error while creating KafkaConsumer ", t);
        }
    }
}