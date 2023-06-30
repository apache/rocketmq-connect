##### EmailSinkConnector fully-qualified name
org.apache.rocketmq.connect.email.sink.EmailSinkConnector

**email-sink-connector** start

```shell
POST  http://${runtime-ip}:${runtime-port}/connectors/EmailSinkConnector
{
    "connector.class": "org.apache.rocketmq.connect.email.sink.EmailSinkConnector",
    "max.task": "1",
    "fromAddress": "xxxxxx@qq.com",
    "toAddress": "xxxxxx@gmail.com",
    "host": "smtp.qq.com",
    "password": "******",
    "subject": "This is a subject",
    "content": "this is a content.",
    "transportProtocol": "smtp",
    "smtpAuth": true,
    "connect.topicnames": "test-mail-topic",
    "value.converter": "org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter",
    "key.converter": "org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter"
}
```

> **Note:** data will be send as a attachment called `emailsink.csv` with email.

##### parameter configuration

| parameter         | effect                                                         | required | default |
| ----------------- | -------------------------------------------------------------- | -------- | ------- |
| fromAddress       | Sender email address                                           | yes      | null    |
| toAddress         | Receiver email address                                         | yes      | null    |
| host              | URL to connect to.                                             | yes      | null    |
| password          | authorization code,You can obtain it from the mailbox Settings | yes      | null    |
| subject           | The subject line of the entire message                         | yes      | null    |
| content           | The body of the entire message                                 | yes      | null    |
| transportProtocol | The protocol to load the session. connector to write into      | yes      | null    |
| smtpAuth          | Whether to authenticate the customer                           | yes      | null    |
