package org.apache.rocketmq.connect.http.sink.signature;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.util.Base64;

public class PushSignatureBuilder {

    public static String signByHmacSHA1(String content, String secret) throws Exception {
        byte[] data = secret.getBytes(StandardCharsets.UTF_8);
        SecretKey secretKey = new SecretKeySpec(data, PushSignatureConstants.HMAC_SHA1_CONSTANT);
        Mac mac = Mac.getInstance(PushSignatureConstants.HMAC_SHA1_CONSTANT);
        mac.init(secretKey);

        byte[] text = content.getBytes(StandardCharsets.UTF_8);
        return Base64.getEncoder()
                .encodeToString(mac.doFinal(text));
    }

    public static String signWithRSA(String content, PrivateKey privateKey) throws Exception {
        Cipher cipher = Cipher.getInstance(PushSignatureConstants.RSA_CONSTANT);
        cipher.init(Cipher.ENCRYPT_MODE, privateKey);
        byte[] b1 = cipher.doFinal(content.getBytes());
        return Base64.getEncoder()
                .encodeToString(b1);
    }
}
