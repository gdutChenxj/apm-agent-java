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
package co.elastic.apm.agent.rocketmq.instrumentation.consumer;

import co.elastic.apm.agent.impl.ElasticApmTracer;
import co.elastic.apm.agent.impl.transaction.Transaction;
import co.elastic.apm.agent.rocketmq.helper.RocketMQInstrumentationHelper;
import co.elastic.apm.agent.rocketmq.instrumentation.BaseRocketMQInstrumentation;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.named;

public class RocketMQConsumeStatusInstrumentation extends BaseRocketMQInstrumentation {


    private static Logger logger = LoggerFactory.getLogger(RocketMQConsumeStatusInstrumentation.class);

    public RocketMQConsumeStatusInstrumentation(ElasticApmTracer tracer) {
        super(tracer);
    }

    @Override
    public ElementMatcher<? super TypeDescription> getTypeMatcher() {
        return hasSuperType(named("org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly")
            .or(named("org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently")));
    }

    @Override
    public ElementMatcher<? super MethodDescription> getMethodMatcher() {
        return named("consumeMessage");
    }

    @Override
    public Class<?> getAdviceClass() {
        return RocketMQConcurrentlyPushConsumerAdvice.class;
    }

    public static class RocketMQConcurrentlyPushConsumerAdvice {

        @Advice.OnMethodEnter(suppress = Throwable.class)
        public static void onBeforeConsumeMessage(@Advice.Local("transaction") Transaction transaction,
                                                  @Advice.Argument(value = 0, readOnly = false) List<MessageExt> msgs) {
            if (tracer == null || tracer.currentTransaction() != null) {
                return;
            }

            if (rocketMQConfig.shouldCollectConsumeProcess()) {
                return;
            }

            if (msgs != null && helperClassManager != null) {
                final RocketMQInstrumentationHelper helper = helperClassManager.getForClassLoaderOfClass(MQProducer.class);
                if (helper == null) {
                    return;
                }

                transaction = helper.onMessageListenerConsume(msgs);
            }
        }

        @Advice.OnMethodExit(suppress = Throwable.class)
        public static void onAfterConsumeMessage(@Advice.Local("transaction") Transaction transaction,
                                                 @Advice.Return(readOnly = false) ConsumeConcurrentlyStatus status) {
            if (tracer == null) {
                return;
            }
            try {
                if (transaction != null && "messaging".equals(transaction.getType())) {
                    transaction.withResult(status.name());
                    transaction.deactivate().end();
                }
            } catch (Exception e) {
                logger.error("Error in RocketMQ consume transaction creation.", e);
            }
        }

    }

}
