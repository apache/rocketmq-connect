/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.rocketmq.replicator.context;


import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.concurrent.atomic.AtomicInteger;

public class UnAckMessage {
    private AtomicInteger unAckCounter;
    private MessageExt msg;
    private long currentOffset;
    private Long putTimestamp;
    private MessageQueue mq;
    public UnAckMessage(int ackCount, MessageExt msg, long currentOffset, MessageQueue mq) {
        this.unAckCounter = new AtomicInteger(ackCount);
        this.msg = msg;
        this.currentOffset = currentOffset;
        this.putTimestamp = System.currentTimeMillis();
        this.mq = mq;
    }

    public int addAndGet(int count) {
        return unAckCounter.addAndGet(count);
    }

    public int get() {
        return unAckCounter.get();
    }

    public long getPutTimestamp() {
        return putTimestamp;
    }

    public MessageExt getMsg() {
        return msg;
    }

    @Override
    public String toString() {
        return "UnAckMessage{" +
                ", msg='" + msg + '\'' +
                ", currentOffset=" + currentOffset +
                ", mq=" + mq +
                '}';
    }
}
