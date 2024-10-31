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
package org.apache.rocketmq.connect.doris.converter.type.connect;

import io.openmessaging.connector.api.data.Schema;
import java.nio.ByteBuffer;
import org.apache.rocketmq.connect.doris.converter.type.doris.DorisType;

public class ConnectBytesType extends AbstractConnectSchemaType {

    public static final ConnectBytesType INSTANCE = new ConnectBytesType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {"BYTES"};
    }

    @Override
    public Object getValue(Object sourceValue) {
        if (sourceValue == null) {
            return null;
        }
        return bytesToHexString(getByteArrayFromValue(sourceValue));
    }

    @Override
    public String getTypeName(Schema schema) {
        return DorisType.STRING;
    }

    private byte[] getByteArrayFromValue(Object value) {
        byte[] byteArray = null;
        if (value instanceof ByteBuffer) {
            final ByteBuffer buffer = ((ByteBuffer) value).slice();
            byteArray = new byte[buffer.remaining()];
            buffer.get(byteArray);
        } else if (value instanceof byte[]) {
            byteArray = (byte[]) value;
        }
        return byteArray;
    }

    /**
     * Convert hexadecimal byte array to string
     */
    private String bytesToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }
}
