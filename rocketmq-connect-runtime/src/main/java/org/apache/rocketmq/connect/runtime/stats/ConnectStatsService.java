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
package org.apache.rocketmq.connect.runtime.stats;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectStatsService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_CONNECT_STATS);

    private static final int FREQUENCY_OF_SAMPLING = 1000;

    private static final int MAX_RECORDS_OF_SAMPLING = 60 * 10;

    private static int printTPSInterval = 60 * 1;

    private final ConcurrentMap<String, AtomicLong> sourceTaskTimesTotal =
            new ConcurrentHashMap<String, AtomicLong>(128);
    private final ConcurrentMap<String, AtomicLong> sinkTaskTimesTotal =
            new ConcurrentHashMap<String, AtomicLong>(128);

    private final LinkedList<CallSnapshot> sourceTaskTimesList = new LinkedList<CallSnapshot>();
    private final LinkedList<CallSnapshot> sinkTaskTimesList = new LinkedList<CallSnapshot>();

    private long connectBootTimestamp = System.currentTimeMillis();
    private ReentrantLock lockPut = new ReentrantLock();
    private ReentrantLock lockGet = new ReentrantLock();

    private volatile long dispatchMaxBuffer = 0;

    private ReentrantLock lockSampling = new ReentrantLock();
    private long lastPrintTimestamp = System.currentTimeMillis();

    public ConnectStatsService() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(1024);
        Long totalTimes = sourceTaskTimesTotal();
        if (0 == totalTimes) {
            totalTimes = 1L;
        }
        Long sinktotalTimes = sinkTaskTimesTotal();
        if (0 == sinktotalTimes) {
            sinktotalTimes = 1L;
        }
        sb.append("\truntime: " + this.getFormatRuntime() + "\r\n");
        sb.append("\tsourceTaskTimesTotal: " + totalTimes + "\r\n");
        sb.append("\tsinTaskTimesTotal: " + sinktotalTimes + "\r\n");
        sb.append("\tsourceTps: " + this.getSourceTaskTps() + "\r\n");
        sb.append("\tgetTotalTps: " + this.getTotalTps() + "\r\n");
        sb.append("\tsinkTps: " + this.getSinkTaskTps() + "\r\n");
        return sb.toString();
    }

    public long sourceTaskTimesTotal() {
        long rs = 0;
        for (AtomicLong data : sourceTaskTimesTotal.values()) {
            rs += data.get();
        }
        return rs;
    }

    public long sinkTaskTimesTotal() {
        long rs = 0;
        for (AtomicLong data : sinkTaskTimesTotal.values()) {
            rs += data.get();
        }
        return rs;
    }

    private String getFormatRuntime() {
        final long millisecond = 1;
        final long second = 1000 * millisecond;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        final long day = 24 * hour;
        final MessageFormat messageFormat = new MessageFormat("[ {0} days, {1} hours, {2} minutes, {3} seconds ]");

        long time = System.currentTimeMillis() - this.connectBootTimestamp;
        long days = time / day;
        long hours = (time % day) / hour;
        long minutes = (time % hour) / minute;
        long seconds = (time % minute) / second;
        return messageFormat.format(new Long[]{days, hours, minutes, seconds});
    }


    private String getSourceTaskTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getSourceTaskTps(10));
        sb.append(" ");

        sb.append(this.getSourceTaskTps(60));
        sb.append(" ");

        sb.append(this.getSourceTaskTps(600));

        return sb.toString();
    }

    private String getSinkTaskTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getSinkTaskTps(10));
        sb.append(" ");

        sb.append(this.getSinkTaskTps(60));
        sb.append(" ");

        sb.append(this.getSinkTaskTps(600));

        return sb.toString();
    }

    private String getTotalTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getTotalTps(10));
        sb.append(" ");

        sb.append(this.getTotalTps(60));
        sb.append(" ");

        sb.append(this.getTotalTps(600));

        return sb.toString();
    }

    private String getSourceTaskTps(int time) {
        String result = "";
        this.lockSampling.lock();
        try {
            CallSnapshot last = this.sourceTaskTimesList.getLast();

            if (this.sourceTaskTimesList.size() > time) {
                CallSnapshot lastBefore = this.sourceTaskTimesList.get(this.sourceTaskTimesList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.lockSampling.unlock();
        }
        return result;
    }

    private String getSinkTaskTps(int time) {
        String result = "";
        this.lockSampling.lock();
        try {
            CallSnapshot last = this.sinkTaskTimesList.getLast();

            if (this.sinkTaskTimesList.size() > time) {
                CallSnapshot lastBefore = this.sinkTaskTimesList.get(this.sinkTaskTimesList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.lockSampling.unlock();
        }
        return result;
    }

    private String getTotalTps(int time) {
        this.lockSampling.lock();
        double source = 0;
        double sink = 0;
        try {
            {
                CallSnapshot last = this.sourceTaskTimesList.getLast();

                if (this.sourceTaskTimesList.size() > time) {
                    CallSnapshot lastBefore = this.sourceTaskTimesList.get(this.sourceTaskTimesList.size() - (time + 1));
                    source += CallSnapshot.getTPS(lastBefore, last);
                }
            }
            {
                CallSnapshot last = this.sinkTaskTimesList.getLast();

                if (this.sinkTaskTimesList.size() > time) {
                    CallSnapshot lastBefore = this.sinkTaskTimesList.get(this.sinkTaskTimesList.size() - (time + 1));
                    sink += CallSnapshot.getTPS(lastBefore, last);
                }
            }
        } finally {
            this.lockSampling.unlock();
        }
        return Double.toString(source + sink);
    }

    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = new HashMap<String, String>(64);

        Long totalTimes = sourceTaskTimesTotal();
        if (0 == totalTimes) {
            totalTimes = 1L;
        }

        Long sinktotalTimes = sinkTaskTimesTotal();
        if (0 == sinktotalTimes) {
            sinktotalTimes = 1L;
        }

        result.put("bootTimestamp", String.valueOf(this.connectBootTimestamp));
        result.put("runtime", this.getFormatRuntime());
        result.put("sourceTaskTimesTotal", String.valueOf(totalTimes));
        result.put("sinkTaskTimesTotal", String.valueOf(sinktotalTimes));
        result.put("sourceTaskTps", String.valueOf(this.getSourceTaskTps()));
        result.put("sinkTaskTps", String.valueOf(this.getSinkTaskTps()));
        return result;
    }

    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(FREQUENCY_OF_SAMPLING);

                this.sampling();

                this.printTps();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return ConnectStatsService.class.getSimpleName();
    }

    private void sampling() {
        this.lockSampling.lock();
        try {
            this.sourceTaskTimesList.add(new CallSnapshot(System.currentTimeMillis(), sourceTaskTimesTotal()));
            if (this.sourceTaskTimesList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.sourceTaskTimesList.removeFirst();
            }

            this.sinkTaskTimesList.add(new CallSnapshot(System.currentTimeMillis(), sinkTaskTimesTotal()));
            if (this.sourceTaskTimesList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.sourceTaskTimesList.removeFirst();
            }
        } finally {
            this.lockSampling.unlock();
        }
    }

    private void printTps() {
        if (System.currentTimeMillis() > (this.lastPrintTimestamp + printTPSInterval * 1000)) {
            this.lastPrintTimestamp = System.currentTimeMillis();

            log.info("[CONNECTPS] source_task_tps {} sink_task_tps {}",
                    this.getSourceTaskTps(printTPSInterval),
                    this.getSinkTaskTps(printTPSInterval)
            );
        }
    }

    public AtomicLong singleSourceTaskTimesTotal(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return null;
        }
        AtomicLong rs = sourceTaskTimesTotal.get(taskId);
        if (null == rs) {
            rs = new AtomicLong(0);
            AtomicLong previous = sourceTaskTimesTotal.putIfAbsent(taskId, rs);
            if (previous != null) {
                rs = previous;
            }
        }
        return rs;
    }

    public AtomicLong singleSinkTaskTimesTotal(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return null;
        }
        AtomicLong rs = sinkTaskTimesTotal.get(taskId);
        if (null == rs) {
            rs = new AtomicLong(0);
            AtomicLong previous = sinkTaskTimesTotal.putIfAbsent(taskId, rs);
            if (previous != null) {
                rs = previous;
            }
        }
        return rs;
    }

    public Map<String, AtomicLong> getSourceTaskTimesTotal() {
        return sourceTaskTimesTotal;
    }

    public Map<String, AtomicLong> getSinkTaskTimesTotal() {
        return sinkTaskTimesTotal;
    }


    static class CallSnapshot {
        public final long timestamp;
        public final long callTimesTotal;

        public CallSnapshot(long timestamp, long callTimesTotal) {
            this.timestamp = timestamp;
            this.callTimesTotal = callTimesTotal;
        }

        public static double getTPS(final CallSnapshot begin, final CallSnapshot end) {
            long total = end.callTimesTotal - begin.callTimesTotal;
            Long time = end.timestamp - begin.timestamp;

            double tps = total / time.doubleValue();

            return tps * 1000;
        }
    }
}
