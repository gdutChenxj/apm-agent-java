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
package co.elastic.apm.agent.rocketmq.helper;

import co.elastic.apm.agent.impl.ElasticApmTracer;
import co.elastic.apm.agent.impl.transaction.Span;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class RocketMQInstrumentationHelperImpl implements RocketMQInstrumentationHelper {

    private final ElasticApmTracer tracer;

    public RocketMQInstrumentationHelperImpl(ElasticApmTracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public SendCallback wrapSendCallback(SendCallback delegate, Span span) {
        if (delegate == null) {
            return null;
        }
        if (delegate instanceof SendCallbackWrapper) {
            return delegate;
        }
        return new SendCallbackWrapper(delegate, span);
    }

    @Override
    public List<MessageExt> wrapMsgFoundList(List<MessageExt> delegate) {
        if (delegate == null) {
            return null;
        }
        if (delegate instanceof ConsumerMessageListWrapper) {
            return delegate;
        }
        return new ConsumerMessageListWrapper(delegate, tracer);
    }

}
