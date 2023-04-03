package org.apache.rocketmq.connect.jdbc.connector.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.connector.api.data.Struct;

public class JdbcSinkTest {
    static String test="{\"schema\":{\"fieldsByName\":{\"money\":{\"schema\":{\"optional\":true,\"fieldType\":\"FLOAT64\"},\"name\":\"money\",\"index\":5},\"name\":{\"schema\":{\"optional\":true,\"fieldType\":\"STRING\"},\"name\":\"name\",\"index\":1},\"begin_time\":{\"schema\":{\"name\":\"io.openmessaging.connector.api.data.logical.Timestamp\",\"optional\":true,\"fieldType\":\"INT64\",\"version\":1},\"name\":\"begin_time\",\"index\":6},\"company\":{\"schema\":{\"optional\":true,\"fieldType\":\"STRING\"},\"name\":\"company\",\"index\":4},\"id\":{\"schema\":{\"optional\":true,\"fieldType\":\"INT64\"},\"name\":\"id\",\"index\":0},\"howold\":{\"schema\":{\"optional\":true,\"fieldType\":\"INT32\"},\"name\":\"howold\",\"index\":2},\"male\":{\"schema\":{\"optional\":true,\"fieldType\":\"INT32\"},\"name\":\"male\",\"index\":3}},\"name\":\"employee_copy1_copy1\",\"optional\":true,\"fields\":[{\"schema\":{\"optional\":true,\"fieldType\":\"INT64\"},\"name\":\"id\",\"index\":0},{\"schema\":{\"optional\":true,\"fieldType\":\"STRING\"},\"name\":\"name\",\"index\":1},{\"schema\":{\"optional\":true,\"fieldType\":\"INT32\"},\"name\":\"howold\",\"index\":2},{\"schema\":{\"optional\":true,\"fieldType\":\"INT32\"},\"name\":\"male\",\"index\":3},{\"schema\":{\"optional\":true,\"fieldType\":\"STRING\"},\"name\":\"company\",\"index\":4},{\"schema\":{\"optional\":true,\"fieldType\":\"FLOAT64\"},\"name\":\"money\",\"index\":5},{\"schema\":{\"name\":\"io.openmessaging.connector.api.data.logical.Timestamp\",\"optional\":true,\"fieldType\":\"INT64\",\"version\":1},\"name\":\"begin_time\",\"index\":6}],\"fieldType\":\"STRUCT\"},\"values\":[3,null,0,0,\"company\",9876.0,1640937600000]}";

    public static void main(String[] args) {
        Struct struct = JSON.parseObject(test, Struct.class);
        JSONObject object = JSON.parseObject(test);
        Object[] values=object.getJSONArray("values").toArray();
        struct.setValues(values);
        System.out.println(object);
    }

}
