package org.apache.rocketmq.connect.runtime.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;

import java.nio.charset.StandardCharsets;

public class FastJsonTest {
    public static void main(String[] args) {
        TestByte testByte = new TestByte();
        byte[] originalJson = JSON.toJSONBytes(testByte);
        JSONObject str = (JSONObject) JSON.parse(originalJson);
        byte[] byte1 = TypeUtils.castToBytes(str.get("bytes").toString());
        System.out.println(new java.lang.String(byte1, StandardCharsets.UTF_8));
    }

    static class TestByte {
        private Object bytes = "123".getBytes(StandardCharsets.UTF_8);

        public Object getBytes() {
            return bytes;
        }

        public void setBytes(Object bytes) {
            this.bytes = bytes;
        }
    }

}
