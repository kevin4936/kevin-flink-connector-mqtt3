/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kevin.flink.streaming.connectors.mqtt;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.kevin.flink.streaming.connectors.mqtt.internal.Retry;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Sink class for writing data into MQTT client.
 * <p>
 * To create an instance of MQTTStreamSink class one should initialize and configure an
 * instance of a connection factory that will be used to create a connection.
 * Every input message is converted into a byte array using a serialization
 * schema and being sent into a message queue.
 *
 * @param <MQTTMessage> type of input messages
 *
 */

 /**
 * author: kevin4936@163.com
 * 
 * 
 */

class MQTTStreamSink extends RichSinkFunction<MQTTMessage> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MQTTStreamSink.class);

    private Map<String, String> config;
    private LocalMessageStore<MQTTMessage> store;
    private SimpleObjectSerializer<MQTTMessage> objectSerializer;

    public MQTTStreamSink(Map<String, String> config) {
        this.config = config;
    }

    /**
     *
     * @param input The incoming data
     */
    @Override
    public void invoke(MQTTMessage input, Context context) throws Exception {
        Integer publishAttempts = Integer.valueOf(System.getProperty("flink.mqtt.client.publish.attempts", "-1"));
        Long publishBackoff = Long.valueOf(System.getProperty("flink.mqtt.client.publish.backoff", "5000")); // 5s
        Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>> tuple2 = CachedMQTTClient.getInstance().getOrCreate(config);
        MqttAsyncClient client = tuple2.f0;
        Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer> configParams = tuple2.f1;
        String topic = configParams.f2;
        Integer qos = configParams.f5;
        //Long messageId = (new Date()).getTime();
        Long messageId = Long.valueOf(input.getMessageId());
        store.store(messageId, input);
        Optional<Boolean> isOkOpt = Retry.apply(publishAttempts, publishBackoff, new Class[] { MqttException.class },
                messageId, id -> {
                    // In case of errors, retry sending the message.
                    try {
                        MQTTMessage message = store.retrieve(id);
                        if(client.isConnected()){
                            client.publish(topic, message.getPayload(), qos, false);
                            return Optional.of(true);
                        }
                        return Optional.of(false);
                    } catch (Exception e) {
                        LOG.error("MQTT client publish error: ", e);
                        return Optional.of(false);
                    }
                });
        if (isOkOpt.orElse(false)) {
            store.remove(messageId);
        }
    }

    /**
     *
     *
     *
     * @throws IllegalArgumentException
     *
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>>  tuple2 = CachedMQTTClient.getInstance().getOrCreate(config);
            objectSerializer = new SimpleObjectSerializer<MQTTMessage>();
            store = new LocalMessageStore<MQTTMessage>(tuple2.f1.f3, objectSerializer, objectSerializer);
        } catch (Exception e) {
            LOG.error("MQTT has not been properly initialized: ", e);
            throw e;
        }
    }

    /**
     * Closes commands container.
     * 
     * @throws IOException if command container is unable to close.
     */
    @Override
    public void close() throws IOException {

    }
}
