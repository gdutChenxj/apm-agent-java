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

import co.elastic.apm.agent.impl.transaction.Transaction;
import org.apache.rocketmq.common.message.MessageExt;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

abstract class MessageListenerWrapper<S, C> {

    @Nonnull
    private Function<List<MessageExt>, Transaction> startTransFunc;

    @Nonnull
    private BiConsumer<Transaction, S> endTransFunc;

    MessageListenerWrapper(@Nonnull Function<List<MessageExt>, Transaction> startTransFunc,
                           @Nonnull BiConsumer<Transaction, S> endTransFunc) {
        this.startTransFunc = startTransFunc;
        this.endTransFunc = endTransFunc;
    }

    S doConsumeMessage(List<MessageExt> msgs, C context) {
        Transaction consumeTrans = startTransFunc.apply(msgs);
        S ret = null;
        try {
            ret = consumeDelegateImpl(msgs, context);
            return ret;
        } catch (Exception exp) {
            consumeTrans.captureException(exp);
            throw exp;
        } finally {
            endTransFunc.accept(consumeTrans, ret);
        }
    }

    abstract S consumeDelegateImpl(List<MessageExt> msgs, C context);

}


