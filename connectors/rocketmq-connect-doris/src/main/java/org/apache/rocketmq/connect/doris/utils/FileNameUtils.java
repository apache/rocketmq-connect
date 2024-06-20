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

package org.apache.rocketmq.connect.doris.utils;

import java.util.Objects;
import org.apache.rocketmq.connect.doris.writer.LoadConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNameUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FileNameUtils.class);

    /**
     * generate file name
     *
     * @param prefix prefix
     * @param end    end offset
     * @return file name
     */
    public static String fileName(String prefix, long end) {
        long time = System.currentTimeMillis();
        String fileName = prefix + end + LoadConstants.FILE_DELIM_DEFAULT + time;
        LOG.debug("generated file name: {}", fileName);
        return fileName;
    }

    /**
     * generate file prefix
     *
     * @param appName   connector name
     * @param partition partition index
     * @return file prefix
     */
    public static String filePrefix(String appName, String topic) {
        return appName
            + LoadConstants.FILE_DELIM_DEFAULT
            + topic
            + LoadConstants.FILE_DELIM_DEFAULT;
    }

    /**
     * verify file name
     *
     * @param fileName file name
     * @return true if file name format is correct, false otherwise
     */
    public static boolean verifyFileName(
        String name, String topic, String fileName) {
        String prefix = filePrefix(name, topic);
        return fileName.startsWith(prefix);
    }

    /**
     * read end offset from file name
     *
     * @param fileName file name
     * @return end offset
     */
    public static long fileNameToEndOffset(String fileName) {
        return Long.parseLong(readFromFileName(fileName, 2));
    }

    public static long labelToEndOffset(String label) {
        return Long.parseLong(readFromFileName(label, 3));
    }

    /**
     * read filename from filepath
     *
     * @param filePath name
     * @return fileName
     */
    public static String fileNameFromPath(String filePath) {
        if (!Objects.isNull(filePath)) {
            int index = filePath.lastIndexOf("/");
            return filePath.substring(index + 1);
        }
        return null;
    }

    /**
     * read a value from file name
     *
     * @param fileName file name
     * @param index    value index
     * @return string value
     */
    private static String readFromFileName(String fileName, int index) {
        String[] splitFileName = fileName.split(LoadConstants.FILE_DELIM_DEFAULT);
        if (splitFileName.length == 0) {
            LOG.warn("The file name does not contain __KC_ and is illegal. fileName={}", fileName);
            return "-1";
        }
        String value = splitFileName[index];
        // Determine whether a string is a number
        if (!value.matches("^[0-9]*$")) {
            LOG.warn("The fileName is not a number. value={}, fileName={}", value, fileName);
            return "-1";
        }
        return value;
    }
}
