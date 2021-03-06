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

package org.wso2.carbon.event.input.adapter.kafka.ha.internal.ds;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.ComponentContext;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterFactory;
import org.wso2.carbon.event.input.adapter.kafka.ha.KafkaEventAdapterFactory;
import org.wso2.carbon.utils.ConfigurationContextService;

/**
 *
 * @scr.component name="input.Kafka09.EventAdaptorService.component" immediate="true"
 * @scr.reference name="configurationcontext.service"
 * interface="org.wso2.carbon.utils.ConfigurationContextService" cardinality="0..1"
 * policy="dynamic" bind="setConfigurationContextService" unbind="unsetConfigurationContextService"
 *
 */
public class KafkaEventAdapterServiceDS {
    private static final Log log = LogFactory.getLog(KafkaEventAdapterServiceDS.class);

    /**
     * initialize the agent service here service here.
     *
     * @param context
     */
    protected void activate(ComponentContext context) {

        try {
            InputEventAdapterFactory kafkaEventEventAdapterFactory = new KafkaEventAdapterFactory();
            context.getBundleContext().registerService(InputEventAdapterFactory.class.getName(),
                    kafkaEventEventAdapterFactory, null);
            if (log.isDebugEnabled()) {
                log.debug("Successfully deployed the input Kafka09 adapter service");
            }

        } catch (RuntimeException e) {
            log.error("Can not create the input Kafka adapter service ", e);
        }
    }

    protected void setConfigurationContextService(
            ConfigurationContextService configurationContextService) {
        KafkaEventAdapterServiceHolder.registerConfigurationContextService(configurationContextService);
    }

    protected void unsetConfigurationContextService(
            ConfigurationContextService configurationContextService) {
        KafkaEventAdapterServiceHolder.unregisterConfigurationContextService(configurationContextService);
    }

}
