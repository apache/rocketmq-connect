/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rocketmq.connect.doris.writer;

/**
 * Abstract class which holds buffered data per partition including its start offset, end offset,
 * size.
 *
 * <p>The getData() method returns the data specific to the implementation.
 *
 * <p>Buffer stores the converted records to Json format.
 *
 * <p>Long lived buffer would mean the data in partition would stay across two put APIs since the
 * buffer thresholds were not met.
 *
 * <p>Please check respective implementation class for more details.
 *
 * @param <T> Return type of {@link #getData()}
 */
public abstract class PartitionBuffer<T> {
    private int numOfRecords;
    private long bufferSizeBytes;
    private long firstOffset;
    private long lastOffset;

    /**
     * @return Number of records in this buffer
     */
    public int getNumOfRecords() {
        return numOfRecords;
    }

    /**
     * @return Buffer size in bytes
     */
    public long getBufferSizeBytes() {
        return bufferSizeBytes;
    }

    /**
     * @return First offset number in this buffer
     */
    public long getFirstOffset() {
        return firstOffset;
    }

    /**
     * @return Last offset number in this buffer
     */
    public long getLastOffset() {
        return lastOffset;
    }

    /**
     * @param numOfRecords Updates number of records (Usually by 1)
     */
    public void setNumOfRecords(int numOfRecords) {
        this.numOfRecords = numOfRecords;
    }

    /**
     * @param bufferSizeBytes Updates sum of size of records present in this buffer (Bytes)
     */
    public void setBufferSizeBytes(long bufferSizeBytes) {
        this.bufferSizeBytes = bufferSizeBytes;
    }

    /**
     * @param firstOffset First offset no to set in this buffer
     */
    public void setFirstOffset(long firstOffset) {
        this.firstOffset = firstOffset;
    }

    /**
     * @param lastOffset Last offset no to set in this buffer
     */
    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

    /**
     * @return true if buffer is empty
     */
    public boolean isEmpty() {
        return numOfRecords == 0;
    }

    /**
     * Public constructor.
     */
    public PartitionBuffer() {
        numOfRecords = 0;
        bufferSizeBytes = 0;
        firstOffset = -1;
        lastOffset = -1;
    }

    /**
     * Inserts the row into Buffer.
     */
    public abstract void insert(String record);

    /**
     * Return the data that was buffered because buffer threshold might have been reached
     *
     * @return respective data type implemented by the class.
     */
    public abstract T getData();

    @Override
    public String toString() {
        return "PartitionBuffer{"
            + "numOfRecords="
            + numOfRecords
            + ", bufferSizeBytes="
            + bufferSizeBytes
            + ", firstOffset="
            + firstOffset
            + ", lastOffset="
            + lastOffset
            + '}';
    }
}
