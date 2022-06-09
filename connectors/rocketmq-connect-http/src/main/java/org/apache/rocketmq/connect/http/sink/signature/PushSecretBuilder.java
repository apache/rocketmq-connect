package org.apache.rocketmq.connect.http.sink.signature;

import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.DefaultConnectError;
import org.apache.commons.lang3.RandomStringUtils;
import sun.misc.BASE64Decoder;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;

public class PushSecretBuilder {

    public static final int DEFAULT_RANDOM_SECRET_LENGTH = 32;

    public static String buildRandomSecret() {
        return RandomStringUtils.randomAlphanumeric(DEFAULT_RANDOM_SECRET_LENGTH);
    }

    public static PrivateKey buildPrivateKey(String certStr) throws ConnectException {
        PrivateKey privateKey;
        try {
            BASE64Decoder base64Decoder = new BASE64Decoder();
            byte[] buffer = base64Decoder.decodeBuffer(certStr);
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(buffer);
            KeyFactory keyFactory = KeyFactory.getInstance(PushSignatureConstants.RSA_CONSTANT);
            privateKey = keyFactory.generatePrivate(keySpec);
        } catch (Throwable e) {
            throw new ConnectException(DefaultConnectError.InternalError, e);
        }
        return privateKey;
    }
}
