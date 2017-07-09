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
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterListener;
import org.wso2.carbon.event.input.adapter.kafka.ha.internal.util.KafkaEventAdapterConstants;
import org.wso2.siddhi.core.SnapshotableElementsHolder;
import org.wso2.siddhi.core.util.snapshot.SnapshotableElement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueConsumer implements Runnable, SnapshotableElement {
    private static final int MAX_RETRY_COUNT = 10;
    private BlockingQueue<KafkaConsumerThread.Event> firstQueue, secondQueue;
    private KafkaConsumerThread.Event e1, e2, last;
    private InputEventAdapterListener brokerListener;
    private List<BlockingQueue<KafkaConsumerThread.Event>> faultyQueues;
    private Map<String, Object> stateInfo;
    private String receiverName;
    private int tenantId;
    private Log LOG = LogFactory.getLog(QueueConsumer.class);

    public QueueConsumer(InputEventAdapterListener inBrokerListener, int tenantId, String receiverName,
                         BlockingQueue<KafkaConsumerThread.Event> firstQueue,
                         BlockingQueue<KafkaConsumerThread.Event> secondQueue) {
        this.brokerListener = inBrokerListener;
        this.tenantId = tenantId;
        this.firstQueue = firstQueue;
        this.secondQueue = secondQueue;
        this.receiverName = receiverName;
        this.faultyQueues = new ArrayList<>(2);
        this.stateInfo = new HashMap<String, Object>();
        SnapshotableElementsHolder.registerSnapshotableElement(this);
    }

    @Override
    public void run() {
        e1 = poll(firstQueue);
        e2 = poll(secondQueue);
        while (true) {
            if (e1 != null && e2 != null) {
                if (e1.compareTo(e2) == 0) {
                    consume(e1);
                    ack(e1);
                    ack(e2);
                    e1 = poll(firstQueue);
                    e2 = poll(secondQueue);
                } else if (e1.compareTo(e2) < 0) {
                    consume(e1);
                    ack(e1);
                    e1 = poll(firstQueue);
                } else {
                    consume(e2);
                    ack(e2);
                    e2 = poll(secondQueue);
                }
            } else if (e1 != null && e2 == null) {
                consume(e1);
                ack(e1);
                e1 = poll(firstQueue);
                e2 = poll(secondQueue); // since e2 is null, we retry
            } else if (e2 != null && e1 == null) {
                consume(e2);
                ack(e2);
                e1 = poll(firstQueue); // since e2 is null, we retry
                e2 = poll(secondQueue);
            } else {
                // both e1, e2 are null, retry
                e1 = poll(firstQueue);
                e2 = poll(secondQueue);
            }
        }
    }

    private void consume(KafkaConsumerThread.Event event) {
        try {
            PrivilegedCarbonContext.startTenantFlow();
            PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantId(tenantId);

            if (last == null || event.compareTo(last) > 0) {
                brokerListener.onEvent(event.getEvent());
                last = event;
            }
        } catch (Throwable t) {
            LOG.error("Error while consuming event : " + event, t);
        } finally {
            PrivilegedCarbonContext.endTenantFlow();
        }
    }

    private void ack(KafkaConsumerThread.Event event) {
        if (event != null) {
            stateInfo.put(event.getQueueId(), event.getOffset());
        }
        if (last != null) {
            stateInfo.put(KafkaEventAdapterConstants.LAST_EVENT_ID_KEY, last.getId());
        }
    }

    private KafkaConsumerThread.Event poll(BlockingQueue<KafkaConsumerThread.Event> queue) {
        boolean faultyQueue = faultyQueues.contains(queue);
        KafkaConsumerThread.Event event = null;

        if (faultyQueue) {
            event = queue.poll();
        } else {
            int retry = 0;
            while (retry < MAX_RETRY_COUNT) {
                try {
                    event = queue.poll(1000, TimeUnit.MILLISECONDS);
                    if (event != null) break;
                } catch (InterruptedException e) {
                    // do nothing
                }
                retry++;
            }
        }

        if (event == null && !faultyQueue) {
            faultyQueues.add(queue);
            LOG.warn(faultyQueues.size() + " of 2 Kafka receivers are not receiving any events.");

            if (faultyQueues.size() > 1) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
        } else if (faultyQueue && event != null) {
            faultyQueues.remove(queue);
            LOG.info("Kafka receiver became active. " + (2 - faultyQueues.size()) + " of 2 Kafka receivers are" +
                    " receiving events.");
        }
        return event;
    }

    @Override
    public Map<String, Object> currentState() {
        return stateInfo;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        LOG.info("************ restoreState ************");
        LOG.info("restoreState: " + state);
        stateInfo = state;
    }

    @Override
    public void onSave(Map<String, Object> state) {
        LOG.info("************ onSaveTriggered ************");
        LOG.info("onSaveState: " + state);
    }

    @Override
    public String getElementId() {
        return receiverName;
    }

}
