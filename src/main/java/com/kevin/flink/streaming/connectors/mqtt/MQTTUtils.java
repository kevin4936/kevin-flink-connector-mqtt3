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

//import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.flink.api.java.tuple.Tuple9;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * author: kevin4936@163.com
 * 
 * 
 */

public class MQTTUtils {

        private static final Logger LOG = LoggerFactory.getLogger(MQTTUtils.class);

        private static Map<String, String> sslParamMapping = new HashMap<String, String>();

        static {
                sslParamMapping.put("ssl.protocol", "com.ibm.ssl.protocol");
                sslParamMapping.put("ssl.key.store", "com.ibm.ssl.keyStore");
                sslParamMapping.put("ssl.key.store.password", "com.ibm.ssl.keyStorePassword");
                sslParamMapping.put("ssl.key.store.type", "com.ibm.ssl.keyStoreType");
                sslParamMapping.put("ssl.key.store.provider", "com.ibm.ssl.keyStoreProvider");
                sslParamMapping.put("ssl.trust.store", "com.ibm.ssl.trustStore");
                sslParamMapping.put("ssl.trust.store.password", "com.ibm.ssl.trustStorePassword");
                sslParamMapping.put("ssl.trust.store.type", "com.ibm.ssl.trustStoreType");
                sslParamMapping.put("ssl.trust.store.provider", "com.ibm.ssl.trustStoreProvider");
                sslParamMapping.put("ssl.ciphers", "com.ibm.ssl.enabledCipherSuites");
                sslParamMapping.put("ssl.key.manager", "com.ibm.ssl.keyManager");
                sslParamMapping.put("ssl.trust.manager", "com.ibm.ssl.trustManager");
        }

        public static Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer> parseConfigParams(
                        Map<String, String> config) {
                // Map<String, String> parameters = new CaseInsensitiveMap(config);
                Map<String, String> parameters = config;
                String brokerUrl = Optional.ofNullable(parameters.get("brokerUrl"))
                                .orElseThrow(() -> new IllegalArgumentException(
                                                "Please provide a `brokerUrl` by specifying path or .options(\"brokerUrl\",...)"));

                MqttClientPersistence persistence = null;
                Optional<String> persistenceOpt = Optional.ofNullable(parameters.get("persistence"));
                if (persistenceOpt.isPresent() && "memory".equals(persistenceOpt.get())) {
                        persistence = new MemoryPersistence();
                } else {
                        Optional<String> localStorageOpt = Optional.ofNullable(parameters.get("localStorage"));
                        if (localStorageOpt.isPresent()) {
                                persistence = new MqttDefaultFilePersistence(localStorageOpt.get());
                        } else {
                                persistence = new MqttDefaultFilePersistence();
                        }
                }

                // if default is subscribe everything, it leads to getting lot unwanted system
                // messages.
                String topic = Optional.ofNullable(parameters.get("topic"))
                                .orElseThrow(() -> new IllegalArgumentException(
                                                "Please specify a topic, by .options(\"topic\",...)"));
                Optional<String> clientIdOpt = Optional.ofNullable(parameters.get("clientId"));
                if(!clientIdOpt.isPresent()){
                        LOG.warn("If `clientId` is not set, a random value is picked up." + "\nRecovering from failure is not supported in such a case.");
                }                                
                String clientId = clientIdOpt.orElse(MqttAsyncClient.generateClientId());
                Optional<String> username = Optional.ofNullable(parameters.get("username"));
                Optional<String> password = Optional.ofNullable(parameters.get("password"));
                int connectionTimeout = Integer.valueOf(Optional.ofNullable(parameters.get("connectionTimeout"))
                                .orElse(String.valueOf(MqttConnectOptions.CONNECTION_TIMEOUT_DEFAULT)));
                int keepAlive = Integer.valueOf(Optional.ofNullable(parameters.get("keepAlive"))
                                .orElse(String.valueOf(MqttConnectOptions.KEEP_ALIVE_INTERVAL_DEFAULT)));
                int mqttVersion = Integer.valueOf(Optional.ofNullable(parameters.get("mqttVersion"))
                                .orElse(String.valueOf(MqttConnectOptions.MQTT_VERSION_DEFAULT)));
                boolean cleanSession = Boolean
                                .valueOf(Optional.ofNullable(parameters.get("cleanSession")).orElse("true"));
                int qos = Integer.valueOf(Optional.ofNullable(parameters.get("QoS")).orElse("2"));
                boolean autoReconnect = Boolean
                                .valueOf(Optional.ofNullable(parameters.get("autoReconnect")).orElse("true"));
                int maxInflight = Integer.valueOf(Optional.ofNullable(parameters.get("maxInflight")).orElse("60"));
                Long maxBatchMessageNum = Long.valueOf(Optional.ofNullable(parameters.get("maxBatchMessageNum"))
                                .orElse(String.valueOf(Long.MAX_VALUE)));
                Long maxBatchMessageSize = Long.valueOf(Optional.ofNullable(parameters.get("maxBatchMessageSize"))
                                .orElse(String.valueOf(Long.MAX_VALUE)));
                int maxRetryNumber = Integer.valueOf(Optional.ofNullable(parameters.get("maxRetryNum")).orElse("3"));

                MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
                mqttConnectOptions.setAutomaticReconnect(autoReconnect);
                mqttConnectOptions.setCleanSession(cleanSession);
                mqttConnectOptions.setConnectionTimeout(connectionTimeout);
                mqttConnectOptions.setKeepAliveInterval(keepAlive);
                mqttConnectOptions.setMqttVersion(mqttVersion);
                mqttConnectOptions.setMaxInflight(maxInflight);
                if (username.isPresent() && password.isPresent()) {
                        mqttConnectOptions.setUserName(username.get());
                        mqttConnectOptions.setPassword(password.get().toCharArray());
                }
                Properties sslProperties = new Properties();
                for (String key : config.keySet()) {
                        if (key.startsWith("ssl.")) {
                                String value = config.get(key);
                                sslProperties.setProperty(sslParamMapping.get(key), value);
                        }
                }
                mqttConnectOptions.setSSLProperties(sslProperties);
                return new Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>(brokerUrl, clientId, topic, persistence, mqttConnectOptions, qos, maxBatchMessageNum,
                                maxBatchMessageSize, maxRetryNumber);
        }

}