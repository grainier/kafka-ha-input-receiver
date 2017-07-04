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


import org.wso2.carbon.event.input.adapter.core.EventAdapterConstants;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapter;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterConfiguration;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterFactory;
import org.wso2.carbon.event.input.adapter.core.MessageType;
import org.wso2.carbon.event.input.adapter.core.Property;
import org.wso2.carbon.event.input.adapter.kafka.ha.internal.util.KafkaEventAdapterConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * The kafka10 event adapter factory class to create a kafka10 input adapter
 */
public class KafkaEventAdapterFactory extends InputEventAdapterFactory {
    private ResourceBundle resourceBundle = ResourceBundle.getBundle(
            "org.wso2.carbon.event.input.adapter.kafka.ha.i18n.Resources", Locale.getDefault());

    @Override
    public String getType() {
        return KafkaEventAdapterConstants.ADAPTOR_TYPE_KAFKA;
    }

    @Override
    public List<String> getSupportedMessageFormats() {
        List<String> supportInputMessageTypes = new ArrayList<String>();
        supportInputMessageTypes.add(MessageType.XML);
        supportInputMessageTypes.add(MessageType.JSON);
        supportInputMessageTypes.add(MessageType.TEXT);
        return supportInputMessageTypes;
    }

    @Override
    public List<Property> getPropertyList() {

        List<Property> propertyList = new ArrayList<Property>();

        //set Zk Connect of broker 1
        Property webZKConnect1 = new Property(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_FIRST_ZOOKEEPER_CONNECT);
        webZKConnect1.setDisplayName(resourceBundle.getString(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_FIRST_ZOOKEEPER_CONNECT));
        webZKConnect1.setHint(resourceBundle.getString(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_ZOOKEEPER_CONNECT_HINT));
        webZKConnect1.setRequired(true);
        propertyList.add(webZKConnect1);

        //set Zk Connect of broker 2
        Property webZKConnect2 = new Property(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_SECOND_ZOOKEEPER_CONNECT);
        webZKConnect2.setDisplayName(resourceBundle.getString(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_SECOND_ZOOKEEPER_CONNECT));
        webZKConnect2.setHint(resourceBundle.getString(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_ZOOKEEPER_CONNECT_HINT));
        webZKConnect2.setRequired(true);
        propertyList.add(webZKConnect2);

        //set GroupID of broker
        Property webGroupID = new Property(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_GROUP_ID);
        webGroupID.setDisplayName(resourceBundle.getString(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_GROUP_ID));
        webGroupID.setHint(resourceBundle.getString(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_GROUP_ID_hint));
        webGroupID.setRequired(true);
        propertyList.add(webGroupID);

        //set Topic of broker
        Property webTopic = new Property(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_TOPIC);
        webTopic.setDisplayName(resourceBundle.getString(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_TOPIC));
        webTopic.setRequired(true);
        propertyList.add(webTopic);

        //set Partition No List
        Property partitionNoList = new Property(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_PARTITION_NO);
        partitionNoList.setDisplayName(resourceBundle.getString(KafkaEventAdapterConstants.ADAPTOR_SUSCRIBER_PARTITION_NO));
        partitionNoList.setRequired(true);
        propertyList.add(partitionNoList);

        Property optionConfigProperties = new Property(KafkaEventAdapterConstants.ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES);
        optionConfigProperties.setDisplayName(
                resourceBundle.getString(KafkaEventAdapterConstants.ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES));
        optionConfigProperties.setHint(resourceBundle.getString(KafkaEventAdapterConstants.ADAPTOR_OPTIONAL_CONFIGURATION_PROPERTIES_HINT));
        propertyList.add(optionConfigProperties);

        // TODO: do we need this property?
        Property isDuplicatedInCluster = new Property(EventAdapterConstants.EVENTS_DUPLICATED_IN_CLUSTER);
        isDuplicatedInCluster.setDisplayName(resourceBundle.getString(EventAdapterConstants.EVENTS_DUPLICATED_IN_CLUSTER));
        isDuplicatedInCluster.setRequired(false);
        isDuplicatedInCluster.setOptions(new String[]{"true", "false"});
        isDuplicatedInCluster.setDefaultValue("false");
        propertyList.add(isDuplicatedInCluster);

        return propertyList;
    }

    @Override
    public String getUsageTips() {
        return null;
    }

    @Override
    public InputEventAdapter createEventAdapter(InputEventAdapterConfiguration eventAdapterConfiguration,
                                                Map<String, String> globalProperties) {
        return new KafkaEventAdapter(eventAdapterConfiguration, globalProperties);
    }
}
