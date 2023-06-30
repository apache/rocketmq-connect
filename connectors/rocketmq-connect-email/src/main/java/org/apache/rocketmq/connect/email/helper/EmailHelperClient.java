/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.email.helper;

import java.io.File;
import java.io.FileWriter;
import java.util.Properties;

import org.apache.rocketmq.connect.email.config.EmailConstants;
import org.apache.rocketmq.connect.email.config.EmailSinkConfig;
import com.sun.mail.util.MailSSLSocketFactory;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Authenticator;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.PasswordAuthentication;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.openmessaging.connector.api.data.ConnectRecord;

public class EmailHelperClient {
    protected final Logger LOGGER = LoggerFactory.getLogger(EmailHelperClient.class);

    private final EmailSinkConfig config;

    private Session mailSession;

    public EmailHelperClient(EmailSinkConfig config) {
        this.config = config;
        createSession(this.config);
    }

    private void createSession(EmailSinkConfig config) {
        Properties properties = new Properties();
        properties.setProperty("mail.host", config.getHost());
        properties.setProperty("mail.transport.protocol", config.getTransportProtocol());
        properties.setProperty("mail.smtp.auth", config.getStmpAuth());
        try {
            MailSSLSocketFactory sf = new MailSSLSocketFactory();
            sf.setTrustAllHosts(true);
            properties.put("mail.smtp.ssl.enable", "true");
            properties.put("mail.smtp.ssl.socketFactory", sf);
            this.mailSession = Session.getDefaultInstance(
                    properties,
                    new Authenticator() {
                        @Override
                        protected PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(
                                    config.getFromAddress(),
                                    config.getPassword());
                        }
                    });
            LOGGER.info("Create email session successfully....");
        } catch (Exception e) {
            throw new RuntimeException("Create email failed", e);
        }
    }

    /**
     * Parse the record as a email and send it
     *
     * @param record the record to be sent
     */
    public void send(ConnectRecord record) {
        createAttachment(record);
        try {
            MimeMessage emailMessage = new MimeMessage(mailSession);

            // Set the sender email address
            emailMessage.setFrom(new InternetAddress(config.getFromAddress()));

            // Set the recipient email address
            emailMessage.addRecipient(
                    Message.RecipientType.TO, new InternetAddress(config.getToAddress()));

            // Set the email subject
            emailMessage.setSubject(config.getSubject());


            // Create message with attachment
            Multipart multipart = new MimeMultipart();

            // Set the email content
            BodyPart contentBodyPart = new MimeBodyPart();
            contentBodyPart.setText(config.getContent());
            multipart.addBodyPart(contentBodyPart);

            // Set the email attachment
            BodyPart fileBodyPart = new MimeBodyPart();
            DataSource source = new FileDataSource(EmailConstants.ATTACHMENT_FILE_NAME);
            fileBodyPart.setDataHandler(new DataHandler(source));
            fileBodyPart.setFileName(EmailConstants.ATTACHMENT_FILE_NAME);
            multipart.addBodyPart(fileBodyPart);

            emailMessage.setContent(multipart);

            // Send a email
            Transport.send(emailMessage);
            LOGGER.info("Sent message successfully....");
        } catch (Exception e) {
            throw new RuntimeException("Send message failed", e);
        }
    }

    /**
     * Create attachment file
     *
     * @param record the record to be sent
     */
    public void createAttachment(ConnectRecord record) {
        try {
            String payload = record.getKey() + "," + record.getData().toString();

            File attachment = new File(EmailConstants.ATTACHMENT_FILE_NAME);
            if (!attachment.exists()) {
                attachment.createNewFile();
            }
            // If the attachment exists, overwrite it
            FileWriter writer = new FileWriter(attachment.getName());
            writer.write(payload);
            writer.close();
            LOGGER.info("Create attachment successfully...");

        } catch (Exception e) {
            throw new RuntimeException("Create attachment file failed", e);
        }

    }
}
