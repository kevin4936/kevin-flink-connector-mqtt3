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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.kevin.flink.streaming.connectors.mqtt.internal.MQTTExceptionListener;
import com.kevin.flink.streaming.connectors.mqtt.internal.RunningChecker;
import org.apache.flink.util.Collector;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class MQTTStreamSourceAndSinkTest {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTStreamSourceAndSinkTest.class);

    //private static final String MQTT_SERVER_TCP = "tcp://127.0.0.1:1883";
    private static final String MQTT_SERVER_TCP = "tcp://127.0.0.1:1883";
    private static final String POST_MESSAGE = "I am flink mqtt message.";
    private static final String CALL_BACK_MESSAGE = "I am flink mqtt callback message.";


    private StreamExecutionEnvironment env;
    private Map<String, String> sourceConfig;

    private Map<String, String> sinkConfig;
    private boolean isRunning = false;
    private  IOTMachine mqttClient;

    @BeforeEach
    public void beforeTest() throws Exception {
        isRunning = true;
        runIOTMachine();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.of(5, TimeUnit.MINUTES),
                Time.of(10, TimeUnit.SECONDS)
        ));
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        String[] args = {"--flink.mqtt.connection.cache.timeout", "150000", "--flink.mqtt.client.publish.attempts", "-1", "--flink.mqtt.client.publish.backoff", "10000" };
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        ExecutionConfig.GlobalJobParameters parameters = env.getConfig().getGlobalJobParameters();
        Map<String, String> map = parameters.toMap();
        String timeout = map.get("flink.mqtt.connection.cache.timeout");
        String attempts = map.get("flink.mqtt.client.publish.attempts");
        String backoff = map.get("flink.mqtt.client.publish.backoff");
        System.setProperty("flink.mqtt.connection.cache.timeout", timeout);
        System.setProperty("flink.mqtt.client.publish.attempts", attempts);
        System.setProperty("flink.mqtt.client.publish.backoff", backoff);

        String password = "test_password";
        //String salt = "secret";
        //String passwordSecret = Hashing.sha256().newHasher().putString(password+salt, Charsets.UTF_8).hash().toString() ;
        sourceConfig = new HashMap<>();
        sourceConfig.put("brokerUrl", MQTT_SERVER_TCP);
        sourceConfig.put("clientId", "flinkMqttSource");
        sourceConfig.put("topic", "flink-mqtt-source");
        sourceConfig.put("username", "test");
        sourceConfig.put("password", password);
        sourceConfig.put("QoS", "2");
        sourceConfig.put("persistence", "memory");
        //sourceConfig.put("localStorage", "/e/mqtt-message");

        sinkConfig = new HashMap<>();
        sinkConfig.put("brokerUrl", MQTT_SERVER_TCP);
        sinkConfig.put("clientId", "flinkMqttSink");
        sinkConfig.put("topic", "flink-mqtt-sink");
        sinkConfig.put("username", "test2");
        sinkConfig.put("password", password);
        sinkConfig.put("QoS", "2");
        sinkConfig.put("persistence", "memory");
    }


    @AfterEach
    public void afterTest() throws Exception {
        isRunning = false;
        stopIOTMachine();
    }


    private void runIOTMachine(){
        try {
            mqttClient = IOTMachine.getInstance();
            mqttClient.subscribeTopic("flink-mqtt-sink");
            mqttClient.setConfig(MQTT_SERVER_TCP, "IOTMachine", "test3", "test_password", 2);
            mqttClient.init(new IMqttCallbackListener() {
                @Override
                public void onSuccess(MqttMessage message) {
                    Assertions.assertEquals(new String(message.getPayload()), CALL_BACK_MESSAGE);
                }

                @Override
                public void onFailure(MqttMessage message, Throwable exception) {

                }
            });
           Thread thread = new Thread(new Runnable() {
               @Override
               public void run() {
                   int count = 0;
                   while (isRunning) {
                       try {
                           mqttClient.publishMessage("flink-mqtt-source", POST_MESSAGE);
                           Thread.sleep(3000);
                       } catch (InterruptedException e) {
                           throw new RuntimeException(e);
                       }
                       if(count>=10) {
                           isRunning = false;
                       }
                       count++;
                   }
               }
           });
           thread.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void stopIOTMachine(){
        mqttClient.closeConnect();
    }

    @Test
    public void testWithSourceAndSink() throws Exception {
        MQTTStreamSource source = new MQTTStreamSource(sourceConfig);
        source.setLogFailuresOnly(true);
        MQTTExceptionListener exceptionListener = new MQTTExceptionListener(LOG, true);
        source.setExceptionListener(exceptionListener);
        source.setRunningChecker(new RunningChecker());
        DataStream<MQTTMessage> stream = env.addSource(source);
        DataStream<MQTTMessage> dataStream = stream
                .flatMap(new FlatMapFunction<MQTTMessage, MQTTMessage>() {

                    @Override
                    public void flatMap(MQTTMessage message, Collector<MQTTMessage> out) throws Exception {
                        Assertions.assertEquals(new String(message.getPayload()), POST_MESSAGE);
                        message.setPayload(CALL_BACK_MESSAGE.getBytes(StandardCharsets.UTF_8));
                        out.collect(message);
                    }

                }).setParallelism(1);
        Configuration configuration = new Configuration();
        ExecutionEnvironment.getExecutionEnvironment().getConfig().setGlobalJobParameters(configuration);
        MQTTStreamSink streamSink = new MQTTStreamSink(sinkConfig);
        dataStream.addSink(streamSink);
        //dataStream.print();
        env.execute();
    }
}
