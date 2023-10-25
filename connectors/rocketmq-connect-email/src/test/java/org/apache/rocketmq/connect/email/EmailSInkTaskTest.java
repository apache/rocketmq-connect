package org.apache.rocketmq.connect.email;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.connect.email.config.EmailConstants;
import org.apache.rocketmq.connect.email.sink.EmailSinkTask;
import org.junit.Assert;
import org.junit.Test;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.internal.DefaultKeyValue;

public class EmailSInkTaskTest {

    // Replace these constants with your own configuration
    private static final String fromAddress = "xxxxxx@qq.com";
    private static final String toAddress = "xxxxx@163.com";
    private static final String host = "smtp.qq.com";
    private static final String password = "******";
    private static final String subject = "Report statistics";
    private static final String content = "This is a test email";
    private static final String transportProtocol = "smtp";
    private static final String stmpAuth = "true";

    @Test
    public void testTask() {
        // Init Task
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(EmailConstants.EMAIL_FROM_ADDRESS, fromAddress);
        keyValue.put(EmailConstants.EMAIL_TO_ADDRESS, toAddress);
        keyValue.put(EmailConstants.EMAIL_HOST, host);
        keyValue.put(EmailConstants.EMAIL_PASSWORD, password);
        keyValue.put(EmailConstants.EMAIL_SUBJECT, subject);
        keyValue.put(EmailConstants.EMAIL_CONTENT, content);
        keyValue.put(EmailConstants.EMAIL_TRANSPORT_PROTOCOL, transportProtocol);
        keyValue.put(EmailConstants.EMAIL_SMTP_AUTH, stmpAuth);

        EmailSinkTask task = new EmailSinkTask();
        task.start(keyValue);

        Assert.assertNotNull(task.getConfig());
        Assert.assertNotNull(task.getEmailClient());

        // Construct records
        List<ConnectRecord> records = new ArrayList<>();
        Schema stringSchema = SchemaBuilder.string().build();
        for (int i = 0; i < 4; i++) {
            ConnectRecord record = new ConnectRecord(null, null, System.currentTimeMillis(), stringSchema, "key" + i,
                    stringSchema, "value" + i);
            records.add(record);
        }

        // Put records
        task.put(records);

        task.stop();
        Assert.assertNull(task.getEmailClient());
    }
}
