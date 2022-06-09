package org.apache.rocketmq.connect.http.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.apache.rocketmq.connect.http.sink.signature.PushSecretBuilder;
import org.apache.rocketmq.connect.http.sink.utils.CheckUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class ApiDestinationSinkConnector extends HttpSinkConnector {
    private static final Logger log = LoggerFactory.getLogger(ApiDestinationSinkConnector.class);

    private String apiDestinationName;

    private static String endpoint;

    private String pushCertPrivateKey;

    private String pushCertSignMethod;

    private String pushCertSignVersion;

    private String pushCertPublicKeyUrl;

    private PrivateKey privateKey;

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        super.taskConfigs(maxTasks);
        List<KeyValue> keyValueList = new ArrayList<>(11);
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(HttpConstant.API_DESTINATION_NAME, apiDestinationName);
        keyValue.put(HttpConstant.ENDPOINT, endpoint);
        keyValue.put(HttpConstant.PUSH_CERT_PRIVATE_KEY, pushCertPrivateKey);
        keyValue.put(HttpConstant.PUSH_CERT_PUBLIC_KEY_URL, pushCertPublicKeyUrl);
        keyValue.put(HttpConstant.PUSH_CERT_SIGN_METHOD, pushCertSignMethod);
        keyValue.put(HttpConstant.PUSH_CERT_SIGN_VERSION, pushCertSignVersion);
        return keyValueList;
    }

    @Override
    public void validate(KeyValue config) {
        if (CheckUtils.checkNull(config.getString(HttpConstant.API_DESTINATION_NAME))
                || CheckUtils.checkNull(config.getString(HttpConstant.ENDPOINT))) {
            throw new RuntimeException("http required parameter is null !");
        }
        try {
            URL urlConnect = new URL(config.getString(HttpConstant.ENDPOINT));
            URLConnection urlConnection = urlConnect.openConnection();
            urlConnection.setConnectTimeout(5000);
            urlConnection.connect();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        super.validate(config);
    }

    @Override
    public void init(KeyValue config) {
        apiDestinationName = config.getString(HttpConstant.API_DESTINATION_NAME);
        endpoint = config.getString(HttpConstant.ENDPOINT);
        pushCertPrivateKey = config.getString(HttpConstant.PUSH_CERT_PRIVATE_KEY);
        pushCertSignMethod = config.getString(HttpConstant.PUSH_CERT_SIGN_METHOD);
        pushCertSignVersion = config.getString(HttpConstant.PUSH_CERT_SIGN_VERSION);
        pushCertPublicKeyUrl = config.getString(HttpConstant.PUSH_CERT_PUBLIC_KEY_URL);
        if (CheckUtils.checkNotNull(pushCertPrivateKey)) {
            String privateKeyStr = new String(Base64.getDecoder()
                    .decode(pushCertPrivateKey), StandardCharsets.UTF_8);
            privateKey = PushSecretBuilder.buildPrivateKey(privateKeyStr);
        }
        super.init(config);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ApiDestinationSinkTask.class;
    }

}
