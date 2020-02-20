/*-
 * #%L
 * Elastic APM Java agent
 * %%
 * Copyright (C) 2018 - 2020 Elastic and contributors
 * %%
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * #L%
 */
package co.elastic.apm.agent.configuration;

import co.elastic.apm.agent.bci.VisibleForAdvice;
import org.stagemonitor.configuration.ConfigurationOption;
import org.stagemonitor.configuration.ConfigurationOptionProvider;

/**
 * Description:
 * Creator: Chenxujian
 * Date: 2020-02-19
 * Time: 10:17 AM
 * Email: chenxujian@cvte.com
 */
public class RocketMQConfiguration extends ConfigurationOptionProvider {

    private static final String MESSAGING_CATEGORY = "Messaging";

    private ConfigurationOption<ConsumerStrategy> consumerStrategy = ConfigurationOption.enumOption(ConsumerStrategy.class)
        .key("rocketmq_consumer_strategy")
        .configurationCategory(MESSAGING_CATEGORY)
        .dynamic(true)
        .buildWithDefault(ConsumerStrategy.CONCURRENTLY_PUSH);


    public ConsumerStrategy getConsumerStrategy() {
        return consumerStrategy.get();
    }

    @VisibleForAdvice
    public enum ConsumerStrategy {
        ORDERLY_PUSH,
        CONCURRENTLY_PUSH,
        PULL
    }

}
