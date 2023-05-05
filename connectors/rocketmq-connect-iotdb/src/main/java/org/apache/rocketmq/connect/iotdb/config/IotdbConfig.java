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
 */

package org.apache.rocketmq.connect.iotdb.config;

import io.openmessaging.KeyValue;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IotdbConfig {

    private Logger log = LoggerFactory.getLogger(IotdbConfig.class);

    public static final Set<String> REQUEST_CONFIG = new HashSet<String>() {
        {
            add(IotdbConstant.IOTDB_HOST);
            add(IotdbConstant.IOTDB_PORT);
            add(IotdbConstant.IOTDB_PATHS);
        }
    };

    private String iotdbHost;

    private Integer iotdbPort;

    /**
     * Separate multiple paths by commas (,) e.g. root.ln.wf01.wt01,root.ln.wf01.wt02,root.ln.wf01.wt03
     */
    private String iotdbPaths;

    public String getIotdbHost() {
        return iotdbHost;
    }

    public void setIotdbHost(String iotdbHost) {
        this.iotdbHost = iotdbHost;
    }

    public Integer getIotdbPort() {
        return iotdbPort;
    }

    public void setIotdbPort(Integer iotdbPort) {
        this.iotdbPort = iotdbPort;
    }

    public String getIotdbPaths() {
        return iotdbPaths;
    }

    public void setIotdbPaths(String iotdbPaths) {
        this.iotdbPaths = iotdbPaths;
    }

    public void load(KeyValue props) {
        properties2Object(props, this);
    }

    private void properties2Object(final KeyValue p, final Object object) {

        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String mn = method.getName();
            if (mn.startsWith("set")) {
                try {
                    String tmp = mn.substring(4);
                    String first = mn.substring(3, 4);

                    String key = first.toLowerCase() + tmp;
                    String property = p.getString(key);
                    if (property != null) {
                        Class<?>[] pt = method.getParameterTypes();
                        if (pt != null && pt.length > 0) {
                            String cn = pt[0].getSimpleName();
                            Object arg;
                            if (cn.equals("int") || cn.equals("Integer")) {
                                arg = Integer.parseInt(property);
                            } else if (cn.equals("long") || cn.equals("Long")) {
                                arg = Long.parseLong(property);
                            } else if (cn.equals("double") || cn.equals("Double")) {
                                arg = Double.parseDouble(property);
                            } else if (cn.equals("boolean") || cn.equals("Boolean")) {
                                arg = Boolean.parseBoolean(property);
                            } else if (cn.equals("float") || cn.equals("Float")) {
                                arg = Float.parseFloat(property);
                            } else if (cn.equals("String")) {
                                arg = property;
                            } else {
                                continue;
                            }
                            method.invoke(object, arg);
                        }
                    }
                } catch (Exception e) {
                    log.error("properties2Object failed", e);
                }
            }
        }
    }
}
