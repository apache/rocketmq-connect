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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.errors;


/**
 * error reporter interface
 */
public interface ErrorReporter extends AutoCloseable {

    /**
     * report message
     *
     * @param context
     */
    void report(ProcessingContext context);

    /**
     * close reporters
     */
    @Override
    default void close() {
    }


    /**
     * A logical stage in a Connect pipeline.
     */
    enum Stage {

        /**
         * source poll
         */
        TASK_POLL,

        /**
         * rocketmq produce
         */
        ROCKETMQ_PRODUCE,


        /**
         * sink put
         */
        TASK_PUT,

        /**
         * transform record
         */
        TRANSFORMATION,

        /**
         * converter record
         */
        CONVERTER,

        /**
         * When producing to rocketmq topic
         */
        PRODUCE,

        /**
         * When consuming from a rocketmq topic
         */
        CONSUME
    }
}
