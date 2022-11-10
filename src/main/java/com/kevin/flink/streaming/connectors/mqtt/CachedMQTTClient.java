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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.kevin.flink.streaming.connectors.mqtt.internal.Retry;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


 /**
 * author: kevin4936@163.com
 * 
 * 
 */

public class CachedMQTTClient {

  private static final Logger LOG = LoggerFactory.getLogger(CachedMQTTClient.class);

  private static CachedMQTTClient instance;
  private static LoadingCache<List<Tuple2<String, String>>, Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>>> cache = null;

   public static CachedMQTTClient getInstance() throws Exception {
     if (instance == null) {
       synchronized (CachedMQTTClient.class) {
         if (instance == null) {
           instance = new CachedMQTTClient();
         }
       }
     }
     return instance;
   }

   private CachedMQTTClient(){
    try {
      CacheLoader<List<Tuple2<String, String>>, Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>>> cacheLoader = new CacheLoader<List<Tuple2<String, String>>, Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>>>() {

        @Override
        public Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>> load(List<Tuple2<String, String>> config) throws Exception {
          LOG.debug(String.format("Creating new MQTT client with params: %s", config));
          Map<String, String> configMap = new HashMap<>();
          for (Tuple2<String, String> keyValue : config) {
            configMap.put(keyValue.f0, keyValue.f1);
          }
          return createMqttClient(configMap);
        }
      };

      RemovalListener<List<Tuple2<String, String>>, Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>>> removalListener = new RemovalListener<List<Tuple2<String, String>>, Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>>>() {

        @Override
        public void onRemoval(
                RemovalNotification<List<Tuple2<String, String>>, Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>>> notification) {
          List<Tuple2<String, String>> params = notification.getKey();
          MqttAsyncClient client = notification.getValue().f0;
          Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer> tuple9 = notification.getValue().f1;
          LOG.debug(String.format("Evicting MQTT client %s params: %s, due to %s", client, params, notification.getCause()));
          closeMqttClient(client, tuple9.f3);
        }
      };
      Long cacheExpireTimeout = Long.parseLong(System.getProperty("flink.mqtt.connection.cache.timeout", "600000")); // 10m
      cache = CacheBuilder.newBuilder().expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
              .removalListener(removalListener)
              .build(cacheLoader);
    } catch (Exception e) {
      LOG.error("MQTT CachedMQTTClient Initializes error: ", e);
    }
  }

  private Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer> > createMqttClient(Map<String, String> config)
      throws Exception {
    Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer> configParams = MQTTUtils
        .parseConfigParams(config);
    String brokerUrl = configParams.f0;
    String clientId = configParams.f1;
    MqttClientPersistence persistence = configParams.f3;
    MqttConnectOptions mqttConnectOptions = configParams.f4;
    MqttAsyncClient client = new MqttAsyncClient(brokerUrl, clientId, persistence);
    MqttCallbackExtended callback = new MqttCallbackExtended() {

      @Override
      public void messageArrived(String topic, MqttMessage message) {

      }

      @Override
      public void deliveryComplete(IMqttDeliveryToken token) {
      }

      @Override
      public void connectionLost(Throwable cause) {
        LOG.warn("Connection to mqtt server lost.", cause);
      }

      @Override
      public void connectComplete(boolean reconnect, String serverURI) {
        LOG.info(String.format("Connect complete %s. Is it a reconnect?: %s", serverURI, reconnect));
      }
    };
    client.setCallback(callback);
    Integer connectAttempts = Integer.valueOf(System.getProperty("flink.mqtt.client.publish.attempts", "-1"));
    Long connectBackoff = Long.valueOf(System.getProperty("flink.mqtt.client.publish.backoff", "5000")); // 5s

    Retry.apply(connectAttempts, connectBackoff, new Class[] { MqttException.class }, Optional.empty(), t -> {
      try {
        client.connect(mqttConnectOptions);
      } catch (MqttSecurityException e) {
        LOG.error("MQTT MQTTStreamSource connect MqttSecurityException: ", e);
      } catch (MqttException e) {
        LOG.error("MQTT MQTTStreamSource connect MqttException: ", e);
      }
      return Optional.of(true);
    });
    return new Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>>(client, configParams);
  }

  public void closeMqttClient(MqttAsyncClient client,
      MqttClientPersistence persistence) {
    try {
      if (client.isConnected()) {
        client.disconnect();
      }
      try {
        persistence.close();
      } catch (Throwable e) {
        LOG.warn(String.format("Error while closing MQTT persistent store %s", e.getMessage()), e);
      }
      client.close();
    } catch (Throwable e) {
      LOG.warn(String.format("Error while closing MQTT client %s", e.getMessage()), e);
    }
  }

  public Tuple2<MqttAsyncClient, Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer>> getOrCreate(Map<String, String> parameters) throws Exception {
    return cache.get(mapToSeq(parameters));
  }

  public void close(Map<String, String> parameters) {
    cache.invalidate(mapToSeq(parameters));
  }

  public void clear() {
    LOG.debug("Cleaning MQTT client cache");
    cache.invalidateAll();
  }

  public List<Tuple2<String, String>> mapToSeq(Map<String, String> parameters) {
    return parameters.entrySet().stream().sorted(Comparator.comparing(e -> e.getKey()))
        .map(e -> new Tuple2<String, String>(e.getKey(), e.getValue())).collect(Collectors.toList());
  }
}
