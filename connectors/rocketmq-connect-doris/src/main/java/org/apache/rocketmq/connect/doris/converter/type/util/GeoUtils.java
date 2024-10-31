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

package org.apache.rocketmq.connect.doris.converter.type.util;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.connector.api.data.Struct;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeoUtils.class);

    private GeoUtils() {
    }

    public static Object handleGeoStructData(Object sourceValue) {
        // the Geometry datatype in MySQL will be converted to
        // a String with Json format
        final ObjectMapper objectMapper = new ObjectMapper();
        Struct geometryStruct = (Struct) sourceValue;

        try {
            byte[] wkb = geometryStruct.getBytes("wkb");
            String geoJson = OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)).asGeoJson();
            JsonNode originGeoNode = objectMapper.readTree(geoJson);

            Optional<Integer> srid = Optional.ofNullable(geometryStruct.getInt32("srid"));
            Map<String, Object> geometryInfo = new HashMap<>();
            String geometryType = originGeoNode.get("type").asText();

            geometryInfo.put("type", geometryType);
            if ("GeometryCollection".equals(geometryType)) {
                geometryInfo.put("geometries", originGeoNode.get("geometries"));
            } else {
                geometryInfo.put("coordinates", originGeoNode.get("coordinates"));
            }

            geometryInfo.put("srid", srid.orElse(0));
            return geometryInfo;
        } catch (Exception e) {
            LOGGER.warn("Failed to parse Geometry datatype, converting the value to null", e);
            return null;
        }
    }
}
