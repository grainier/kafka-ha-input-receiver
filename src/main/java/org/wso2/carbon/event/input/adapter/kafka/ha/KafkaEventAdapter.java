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
import org.wso2.carbon.event.input.adapter.core.EventAdapterConstants;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapter;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterConfiguration;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterListener;
import org.wso2.carbon.event.input.adapter.core.exception.InputEventAdapterException;
import org.wso2.carbon.event.input.adapter.core.exception.InputEventAdapterRuntimeException;
import org.wso2.carbon.event.input.adapter.core.exception.TestConnectionNotSupportedException;
import org.wso2.carbon.event.input.adapter.kafka.ha.internal.util.KafkaEventAdapterConstants;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public final class KafkaEventAdapter implements InputEventAdapter {

    private static final Log LOG = LogFactory.getLog(KafkaEventAdapter.class);
    private final InputEventAdapterConfiguration eventAdapterConfiguration;
    private final Map<String, String> globalProperties;
    private final String id = UUID.randomUUID().toString();
    private InputEventAdapterListener eventAdaptorListener;
    private int tenantId;
    private String receiverName;
    private ConsumerKafkaAdaptor consumerKafkaAdaptor;

    public KafkaEventAdapter(InputEventAdapterConfiguration eventAdapterConfiguration,
                             Map<String, String> globalProperties) {
        this.eventAdapterConfiguration = eventAdapterConfiguration;
        this.globalProperties = globalProperties;
        this.receiverName = eventAdapterConfiguration.getName();
    }

    private static Properties getConsumerProperties(String zookeeper, String groupId,
                                                    String optionalConfigs) {
        try {
            Properties props = new Properties();
            props.put(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_ZOOKEEPER_CONNECT, zookeeper);
            props.put(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_GROUP_ID, groupId);
            if (optionalConfigs != null) {
                String[] optionalProperties = optionalConfigs.split(",");
                for (String header : optionalProperties) {
                    String[] configPropertyWithValue = header.split(":", 2);
                    if (configPropertyWithValue.length == 2) {
                        props.put(configPropertyWithValue[0], configPropertyWithValue[1]);
                    } else {
                        LOG.warn("Optional configuration property not defined in the correct format.\n" +
                                "Required - property_name1:property_value1,property_name2:property_value2\n" +
                                "Found - " + optionalConfigs);
                    }
                }
            }

            //TODO Define as global properties
            props.put("session.timeout.ms", "30000");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            // props.put("partition.assignment.strategy", "range");
            return props;
        } catch (NoClassDefFoundError e) {
            throw new InputEventAdapterRuntimeException("Cannot access Kafka context due to missing jars", e);
        }
    }

    @Override
    public void init(InputEventAdapterListener eventAdaptorListener) throws InputEventAdapterException {
        validateInputEventAdapterConfigurations();
        this.eventAdaptorListener = eventAdaptorListener;
    }

    @Override
    public void testConnect() throws TestConnectionNotSupportedException {
        throw new TestConnectionNotSupportedException("not-supported");
    }

    @Override
    public void connect() {
        tenantId = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantId(true);
        createKafkaAdaptorListener(eventAdaptorListener, eventAdapterConfiguration);
    }

    @Override
    public void disconnect() {
        if (consumerKafkaAdaptor != null) {
            consumerKafkaAdaptor.shutdown();
            String topic = eventAdapterConfiguration.getProperties()
                    .get(KafkaEventAdapterConstants.ADAPTER_MESSAGE_TOPIC);
            LOG.debug("Adapter " + eventAdapterConfiguration.getName() + " disconnected " + topic);
        }
    }

    @Override
    public void destroy() {
    }

    public InputEventAdapterListener getEventAdaptorListener() {
        return eventAdaptorListener;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean isEventDuplicatedInCluster() {
        return Boolean.parseBoolean(eventAdapterConfiguration.getProperties()
                .get(EventAdapterConstants.EVENTS_DUPLICATED_IN_CLUSTER));
    }

    @Override
    public boolean isPolling() {
        return true;
    }

    private void validateInputEventAdapterConfigurations() throws InputEventAdapterException {
        String partitionProperty = eventAdapterConfiguration.getProperties()
                .get(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_PARTITION_NO);
        try {
            Integer.parseInt(partitionProperty);
        } catch (NumberFormatException e) {
            throw new InputEventAdapterException("Invalid value set for property 'Partition No': " +
                    partitionProperty, e);
        }
    }

    private void createKafkaAdaptorListener(InputEventAdapterListener inputEventAdapterListener,
                                            InputEventAdapterConfiguration inputEventAdapterConfiguration) {

        Map<String, String> brokerProperties = new HashMap<String, String>();
        brokerProperties.putAll(inputEventAdapterConfiguration.getProperties());
        String zkConnect1 = brokerProperties.get(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_FIRST_ZOOKEEPER_CONNECT);
        String zkConnect2 = brokerProperties.get(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_SECOND_ZOOKEEPER_CONNECT);
        String groupID = brokerProperties.get(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_GROUP_ID);
        String partition = brokerProperties.get(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_PARTITION_NO);
        String optionalConfiguration = brokerProperties.get(KafkaEventAdapterConstants.ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES);
        String topic = inputEventAdapterConfiguration.getProperties().get(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_TOPIC);

        consumerKafkaAdaptor = new ConsumerKafkaAdaptor(
                topic,
                partition,
                tenantId,
                KafkaEventAdapter.getConsumerProperties(zkConnect1, groupID, optionalConfiguration),
                KafkaEventAdapter.getConsumerProperties(zkConnect2, groupID, optionalConfiguration),
                inputEventAdapterConfiguration.getName()
        );
        consumerKafkaAdaptor.run(inputEventAdapterListener);
    }
}
