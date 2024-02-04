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
package org.apache.rocketmq.connect.http.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map.Entry;

public class JsonUtils {
    public final static String QUESTION_MARK = "?";
    public static JSONObject mergeJson(JSONObject source, JSONObject target) {
        if (target == null) {
            return source;
        }
        if (source == null) {
            return target;
        }
        for (String key : source.keySet()) {
            Object value = source.get(key);
            if (!target.containsKey(key)) {
                target.put(key, value);
            } else {
                if (value instanceof JSONObject) {
                    JSONObject valueJson = (JSONObject) value;
                    JSONObject targetValue = mergeJson(valueJson, target.getJSONObject(key));
                    target.put(key, targetValue);
                } else if (value instanceof JSONArray) {
                    JSONArray valueArray = (JSONArray) value;
                    for (int i = 0; i < valueArray.size(); i++) {
                        JSONObject obj = (JSONObject) valueArray.get(i);
                        JSONObject targetValue = mergeJson(obj, (JSONObject) target.getJSONArray(key).get(i));
                        target.getJSONArray(key).set(i, targetValue);
                    }
                } else {
                    target.put(key, value);
                }
            }
        }
        return target;
    }

    public static String addQueryStringAndPathValueToUrl(String url, String queryString) throws UnsupportedEncodingException {
        StringBuilder queryStringBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(queryString)) {
            final JSONObject jsonObject = JSONObject.parseObject(queryString);
            for (Entry<String, Object> next : jsonObject.entrySet()) {
                if (next.getValue() instanceof JSONObject) {
                    queryStringBuilder.append(URLEncoder.encode(next.getKey(), "UTF-8")).append("=").append(URLEncoder.encode(((JSONObject) next.getValue()).toJSONString(), "UTF-8")).append("&");
                } else {
                    queryStringBuilder.append(URLEncoder.encode(next.getKey(), "UTF-8")).append("=").append(URLEncoder.encode((String) next.getValue(), "UTF-8")).append("&");
                }
            }
        }
        String path = queryStringBuilder.toString();
        if (StringUtils.isNotBlank(path) && StringUtils.isNotBlank(url)) {
            if (url.contains(QUESTION_MARK)) {
                return url + "&" + path.substring(0, path.length() - 1);
            }
            return url + "?" + path.substring(0, path.length() - 1);
        }
        return url;
    }
}
