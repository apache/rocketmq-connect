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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException, IOException {

        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.start();

        File s = new File("user.avsc");
        Schema schema = new Schema.Parser().parse(s);

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "osgoo");
        user1.put("favorite_number", 256);
        user1.put("favorite_color", "white");


        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(schema);
        Encoder e = EncoderFactory.get().jsonEncoder(schema, bao);
        w.write(user1, e);
        e.flush();

        for (int i = 0; i < 1; i++)
            try {
                {
                    Message msg = new Message("testhudi1",
                        "TagA",
                        "OrderID188",
                        bao.toByteArray());
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("%s%n", sendResult);
                }

            } catch (Exception e1) {
                e1.printStackTrace();
            }

        producer.shutdown();
    }
}
