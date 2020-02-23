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

import co.elastic.apm.agent.AbstractInstrumentationTest;
import co.elastic.apm.agent.impl.context.Destination;
import co.elastic.apm.agent.impl.context.SpanContext;
import co.elastic.apm.agent.impl.transaction.Span;
import co.elastic.apm.agent.impl.transaction.TraceContext;
import co.elastic.apm.agent.impl.transaction.Transaction;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SuppressWarnings("NotNullFieldNotInitialized")
public class RocketMQInstrumentationTest extends AbstractInstrumentationTest {

    private static final String NAME_SRV = "172.18.70.49:9876";

    private static final String PRODUCER_GROUP =  UUID.randomUUID().toString();

    private static final String CONSUMER_GROUP = UUID.randomUUID().toString();

    private static final String TOPIC = "ApmRocketMQPluginDev";

    private static final byte[] FIRST_MESSAGE_BODY = "First message body".getBytes(StandardCharsets.UTF_8);

    private static final byte[] SECOND_MESSAGE_BODY = "Second message body".getBytes(StandardCharsets.UTF_8);

    private static DefaultMQProducer producer;

    @BeforeClass
    public static void setup() throws MQClientException {
        initProducer();
        initConsumer();
    }

    private static void initProducer() throws MQClientException {
        producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAME_SRV);
        producer.start();
    }

    private static void initConsumer() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAME_SRV);
        consumer.registerMessageListener(new MessageConsumer());
        consumer.subscribe(TOPIC, "*");
        consumer.start();
    }

    @Before
    public void startTransaction() {
        reporter.reset();
        Transaction transaction = tracer.startTransaction(TraceContext.asRoot(), null, null).activate();
        transaction.withName("RocketMQ-Test Transaction");
        transaction.withType("test");
    }

    @After
    public void endTransaction() {
        Transaction currentTransaction = tracer.currentTransaction();
        if (currentTransaction != null) {
            currentTransaction.deactivate().end();
        }
    }

    @Test
    public void testSendAndConsumeMessage() {
        sendTwoMessage();
        List<Span> spans = reporter.getSpans();
        assertThat(spans).hasSize(2);
        spans.forEach(this::verifySendSpanContents);
    }

    private void sendTwoMessage() {
        StringBuilder callbackExecutedFlag = new StringBuilder();

        Message firstMessage = new Message(TOPIC, FIRST_MESSAGE_BODY);
        Message secondMessage = new Message(TOPIC, SECOND_MESSAGE_BODY);
        try {
            producer.send(firstMessage);
            producer.send(secondMessage, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    callbackExecutedFlag.append("success");
                }

                @Override
                public void onException(Throwable e) {
                    callbackExecutedFlag.append("failure");
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        await().atMost(5000, TimeUnit.MILLISECONDS)
            .until(() -> reporter.getTransactions().stream().anyMatch(this::isMessagingTransaction));

        assertThat(callbackExecutedFlag).isNotEmpty();
        verifyTracing();
    }


    private boolean isMessagingTransaction(Transaction transaction) {
        return "messaging".equals(transaction.getType());
    }

    private void verifyTracing() {
        reporter.getTransactions().stream()
            .filter(this::isMessagingTransaction)
            .forEach(this::verifyConsumeTransactionContents);

        List<Span> spans = reporter.getSpans();
        assertThat(spans).hasSize(2);
        spans.forEach(this::verifySendSpanContents);
    }
    
    private void verifyConsumeTransactionContents(Transaction transaction) {
        assertThat(transaction.getType()).isEqualTo("messaging");
        assertThat(transaction.getNameAsString()).startsWith("RocketMQ Message Consume");
        assertThat(transaction.getResult()).isNotEmpty();
    }

    private void verifySendSpanContents(Span span) {
        assertThat(span.getType()).isEqualTo("messaging");
        assertThat(span.getSubtype()).isEqualTo("rocketmq");
        assertThat(span.getAction()).isEqualTo("send");
        assertThat(span.getNameAsString()).isEqualTo("DefaultMQProducerImpl#sendKernelImpl");

        SpanContext context = span.getContext();

        assertThat(context.getMessage().getQueueName()).startsWith(TOPIC);

        Destination.Service service = context.getDestination().getService();
        assertThat(service.getType()).isEqualTo("messaging");
        assertThat(service.getName().toString()).isEqualTo("rocketmq");
        assertThat(service.getResource().toString()).isEqualTo("rocketmq/" + TOPIC);
    }

    static class MessageConsumer implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

}
