package org.apache.rocketmq.connect.oss.config;

import io.openmessaging.KeyValue;
import java.lang.reflect.Method;
@lombok.Getter
@lombok.Setter
public class TaskConfig {

    public static final String ENDPOINT = "endpoint";

    public static final String ACCESS_KEY_ID = "accesskeyid";

    public static final String ACCESS_KEY_SECRET = "accesskeysecret";

    public static final String BUCKET_NAME = "bucketname";

    public static final String PARTITION_TYPE = "partitiontype";

    public static final String BATCH_SEND_TYPE = "batchsendtype";

    public static final String FILE_CONTENT_TYPE = "filecontenttype";

    public static final String BATCH_SEND_TIME_INTERVAL = "timeinterval";

    public static final String BATCH_SEND_GROUP_SIZE = "batchsize";


    private String endpoint;

    private String accessKeyId;

    private String accessKeySecret;

    private String bucketName;

    private String partitionType;

    private String batchSendType;

    private String fileContentType;

    private String timeInterval;

    private String batchSize;


    public void load(KeyValue props) {
        properties2Object(props, this);
    }

    private void properties2Object(final KeyValue properties, final Object object) {

        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String methodName = method.getName();
            if (methodName.startsWith("set")) {
                try {
                    String key = methodName.substring(3).toLowerCase();
                    String property = properties.getString(key);
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
                } catch (Throwable ignored) {
                }
            }
        }
    }


}
