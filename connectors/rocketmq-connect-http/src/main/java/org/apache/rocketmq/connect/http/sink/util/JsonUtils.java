package org.apache.rocketmq.connect.http.sink.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map.Entry;

public class JsonUtils {

    private static final String questionMark = "?";

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

    public static String queryStringAndPathValue(String url, String queryString, String pathValue) throws UnsupportedEncodingException {
        StringBuilder pathValueString = new StringBuilder();
        if (StringUtils.isNotBlank(pathValue)) {
            final JSONArray objects = JSONArray.parseArray(pathValue);
            for (Object object : objects) {
                pathValueString.append(HttpConstant.HTTP_PATH_VALUE)
                        .append("=").append(object).append("&");
            }
        }
        StringBuilder queryStringBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(queryString)) {
            final JSONObject jsonObject = JSONObject.parseObject(queryString);
            for (Entry<String, Object> next : jsonObject.entrySet()) {
                if (next.getValue() instanceof JSONObject) {
                    queryStringBuilder.append(next.getKey()).append("=").append(URLEncoder.encode(((JSONObject) next.getValue()).toJSONString(), "UTF-8")).append("&");
                } else {
                    queryStringBuilder.append(next.getKey()).append("=").append(URLEncoder.encode((String) next.getValue(), "UTF-8")).append("&");
                }
            }
        }
        String path = pathValueString + queryStringBuilder.toString();
        if (StringUtils.isNotBlank(path) && StringUtils.isNotBlank(url)) {
            if (url.contains(questionMark)) {
                return url + "&" + path.substring(0, path.length() - 1);
            }
            return url + "?" + path.substring(0, path.length() - 1);
        }
        return url;
    }
}