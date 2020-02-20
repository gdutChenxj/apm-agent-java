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
package co.elastic.apm.agent.rocketmq;

import co.elastic.apm.agent.configuration.RocketMQConfiguration;
import co.elastic.apm.agent.impl.ElasticApmTracer;
import co.elastic.apm.agent.impl.transaction.Transaction;
import co.elastic.apm.agent.rocketmq.helper.RocketMQInstrumentationHelper;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.rocketmq.client.consumer.MQConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static net.bytebuddy.matcher.ElementMatchers.hasSuperType;
import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Description:
 * Creator: Chenxujian
 * Date: 2020-02-18
 * Time: 11:17 AM
 * Email: chenxujian@cvte.com
 */
public class RocketMQConcurrentlyPushConsumerInstrumentation extends BaseRocketMQInstrumentation {


    private static Logger logger = LoggerFactory.getLogger(RocketMQConcurrentlyPushConsumerInstrumentation.class);

    public RocketMQConcurrentlyPushConsumerInstrumentation(ElasticApmTracer tracer) {
        super(tracer);
        if (tracer.getConfig(RocketMQConfiguration.class).getConsumerStrategy() != RocketMQConfiguration.ConsumerStrategy.PULL) {
            System.setProperty("elastic.apm.disable_instrumentations", "rocketmq");
            System.out.println("修改 elastic.apm.disable_instrumentations 至 rocketmq");
        }
    }

    @Override
    public ElementMatcher<? super TypeDescription> getTypeMatcher() {
        return hasSuperType(named("org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently"));
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
        public static void onBeforeConsumeMessage(@Advice.Argument(value = 0, readOnly = false) List<MessageExt> msgs) {
            if (tracer == null || tracer.currentTransaction() != null) {
                return;
            }

            if (msgs != null && helperClassManager != null) {
                final RocketMQInstrumentationHelper helper = helperClassManager.getForClassLoaderOfClass(MQConsumer.class);
                if (helper == null) {
                    return;
                }

                helper.onMessageListenerConsume(msgs, RocketMQConfiguration.ConsumerStrategy.CONCURRENTLY_PUSH);
            }
        }

        @Advice.OnMethodExit(suppress = Throwable.class)
        public static void onAfterConsumeMessage(@Advice.Return(readOnly = false) ConsumeConcurrentlyStatus status) {
            if (tracer == null) {
                return;
            }
            try {
                Transaction transaction = tracer.currentTransaction();
                if (transaction != null && "messaging".equals(transaction.getType())) {
                    if (status == ConsumeConcurrentlyStatus.RECONSUME_LATER) {
                        transaction.addLabel("consume_status", status.name());
                    }
                    transaction.deactivate().end();
                }
            } catch (Exception e) {
                logger.error("Error in RocketMQ iterator wrapper", e);
            }
        }

    }

}
