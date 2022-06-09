package org.apache.rocketmq.connect.http.sink.signature;

import org.apache.http.message.BasicHeader;

import java.util.HashMap;
import java.util.Map;

import static org.apache.rocketmq.connect.http.sink.signature.PushSignatureConstants.SEPARATOR_OF_STRING_TO_SIGN;


public class PushStringToSignBuilder {

    public static String defaultStringToSign(String url, Map<String, String> officialHeaders, String body) {
        return buildStringToSign(url, officialHeaders, body);
    }

    public static Map<String, String> officialHeaders(String signMethod, String signVersion, String signUrl) {
        Map<String, String> officialHeaders = new HashMap<>(16);
        officialHeaders.put(PushSignatureConstants.HEADER_X_EVENTBRIDGE_SIGNATURE_TIMESTAMP,
                String.valueOf(System.currentTimeMillis()));
        officialHeaders.put(PushSignatureConstants.HEADER_X_EVENTBRIDGE_SIGNATURE_METHOD, signMethod);
        officialHeaders.put(PushSignatureConstants.HEADER_X_EVENTBRIDGE_SIGNATURE_VERSION, signVersion);
        officialHeaders.put(PushSignatureConstants.HEADER_X_EVENTBRIDGE_SIGNATURE_URL, signUrl);
        return officialHeaders;
    }

    private static String buildStringToSign(String url, Map<String, String> headers, String body) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(url)
                .append(SEPARATOR_OF_STRING_TO_SIGN);
        if (headers != null && !headers.isEmpty()) {
            headers.forEach((key, value) -> buffer.append(new BasicHeader(key, value))
                    .append(SEPARATOR_OF_STRING_TO_SIGN));
        }
        buffer.append(body)
                .append(SEPARATOR_OF_STRING_TO_SIGN);
        return buffer.toString();
    }

}
