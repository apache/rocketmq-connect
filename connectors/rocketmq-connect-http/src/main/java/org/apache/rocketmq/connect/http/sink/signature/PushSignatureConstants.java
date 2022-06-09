package org.apache.rocketmq.connect.http.sink.signature;

public class PushSignatureConstants {

    public static final String HEADER_X_EVENTBRIDGE_SIGNATURE_TIMESTAMP = "x-eventbridge-signature-timestamp";
    public static final String HEADER_X_EVENTBRIDGE_SIGNATURE_METHOD = "x-eventbridge-signature-method";
    public static final String HEADER_X_EVENTBRIDGE_SIGNATURE_VERSION = "x-eventbridge-signature-version";
    public static final String HEADER_X_EVENTBRIDGE_SIGNATURE_URL = "x-eventbridge-signature-url";
    public static final String HEADER_X_EVENTBRIDGE_SIGNATURE_SECRET = "x-eventbridge-signature-secret";
    public static final String HEADER_X_EVENTBRIDGE_SIGNATURE = "x-eventbridge-signature";
    public static final String SEPARATOR_OF_STRING_TO_SIGN = "\n";
    public static final String RSA_CONSTANT = "RSA";
    public static final String HMAC_SHA1_CONSTANT = "HmacSHA1";
}
