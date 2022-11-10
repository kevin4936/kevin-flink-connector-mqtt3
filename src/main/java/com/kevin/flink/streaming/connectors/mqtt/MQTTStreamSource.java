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

import com.kevin.flink.streaming.connectors.mqtt.internal.MQTTExceptionListener;
import com.kevin.flink.streaming.connectors.mqtt.internal.RunningChecker;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Source for reading messages from an MQTT client.
 * <p>
 * To create an instance of MQTTStreamSource class one should initialize and
 * configure an
 * instance of a connection factory that will be used to create a connection.
 * This source is waiting for incoming messages from MQTT and converts them from
 * an array of bytes into an instance of the output type. If an incoming
 * message is not a message with an array of bytes, this message is ignored
 * and warning message is logged.
 *
 * If checkpointing is enabled MQTTStreamSource will not acknowledge received
 * AMQ messages as they arrive,
 * but will store them internally and will acknowledge a bulk of messages during
 * checkpointing.
 *
 * @param <MQTTMessage> type of output messages
 *
 */

 /**
 * author: kevin4936@163.com
 * 
 * 
 */

public class MQTTStreamSource extends MessageAcknowledgingSourceBase<MQTTMessage, Long>
    implements ResultTypeQueryable<MQTTMessage> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(MQTTStreamSource.class);

  private Map<String, String> config;
  private MqttAsyncClient client;
  private LocalMessageStore<MQTTMessage> store;
  private BlockingQueue<Long> queue;
  // Stores if source is running (used for testing)
  private RunningChecker runningChecker;
  // If source should immediately acknowledge incoming message
  private boolean autoAck;
  private SimpleObjectSerializer<MQTTMessage> objectSerializer = null;
  // Listener for mqtt exceptions
  private MQTTExceptionListener exceptionListener;
  // Throw exceptions or just log them
  private boolean logFailuresOnly = false;

  public MQTTStreamSource(Map<String, String> config) {
    super(Long.class);
    this.config = config;
    this.objectSerializer = new SimpleObjectSerializer<MQTTMessage>();
    this.runningChecker = new RunningChecker();
    this.exceptionListener = new MQTTExceptionListener(LOG, logFailuresOnly);
  }

  /**
   * Defines whether the producer should fail on errors, or only log them.
   * If this is set to true, then exceptions will be only logged, if set to false,
   * exceptions will be eventually thrown and cause the streaming program to
   * fail (and enter recovery).
   *
   * @param logFailuresOnly The flag to indicate logging-only on exceptions.
   */
  public void setLogFailuresOnly(boolean logFailuresOnly) {
    this.logFailuresOnly = logFailuresOnly;
  }

  // Visible for testing
  public void setExceptionListener(MQTTExceptionListener exceptionListener) {
    this.exceptionListener = exceptionListener;
  }

  public void setRunningChecker(RunningChecker runningChecker) {
    this.runningChecker = runningChecker;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    Tuple9<String, String, String, MqttClientPersistence, MqttConnectOptions, Integer, Long, Long, Integer> configParams = MQTTUtils
        .parseConfigParams(config);
    String brokerUrl = configParams.f0;
    String clientId = configParams.f1;
    String topic = configParams.f2;
    MqttClientPersistence persistence = configParams.f3;
    store = new LocalMessageStore<MQTTMessage>(persistence, objectSerializer, objectSerializer);
    queue = new ArrayBlockingQueue<Long>(10000);
    MqttConnectOptions mqttConnectOptions = configParams.f4;
    Integer qos = configParams.f5;
    client = new MqttAsyncClient(brokerUrl, clientId, persistence);
    MqttCallbackExtended callback = new MqttCallbackExtended() {

      @Override
      public void messageArrived(String topic, MqttMessage message) {
        synchronized (this) {
          try {
            MQTTMessage mqttMessage = new MQTTMessage();
            mqttMessage.setMutable(false);
            mqttMessage.setTopic(topic);
            mqttMessage.setMessageId(message.getId());
            mqttMessage.setRetained(message.isRetained());
            mqttMessage.setDuplicate(message.isDuplicate());
            mqttMessage.setQos(message.getQos());
            mqttMessage.setPayload(message.getPayload());
            //String messageId = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").format(LocalDateTime.now()) + mqttMessage.getMessageId();
            Long messageId = Long.valueOf(mqttMessage.getMessageId());
            store.store(messageId, mqttMessage);
            queue.put(messageId);
            LOG.trace(String.format("Message arrived, %s", mqttMessage.toString()));
          } catch (InterruptedException e) {
            LOG.error("MQTT MQTTStreamSource put message into queue error: ", e);
          }
        }
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
        // It is not possible to initialize offset without `client.connect`
        try {
          client.subscribe(topic, qos);
        } catch (MqttException e) {
          throw new RuntimeException(e);
        }
      }
    };
    client.setCallback(callback);
    client.connect(mqttConnectOptions);
    runningChecker.setIsRunning(true);
  }

  @Override
  public void close() throws Exception {
    super.close();
    RuntimeException exception = null;
    try {
      client.disconnect();
    } catch (MqttException e) {
      if (logFailuresOnly) {
        LOG.error("Failed to disconnect MQTT client", e);
      } else {
        exception = new RuntimeException("Failed to disconnect MQTT client", e);
      }
    }
    try {
      store.close();
    } catch (MqttException e) {
      if (logFailuresOnly) {
        LOG.error("Failed to close MQTT store", e);
      } else {
        exception = exception == null ? new RuntimeException("Failed to close MQTT store", e)
            : exception;
      }

    }
    try {
      client.close();
    } catch (MqttException e) {
      if (logFailuresOnly) {
        LOG.error("Failed to close MQTT client", e);
      } else {
        exception = exception == null ? new RuntimeException("Failed to close MQTT client", e)
            : exception;
      }
    }

    if (exception != null) {
      throw exception;
    }
  }

  @Override
  public void run(SourceContext<MQTTMessage> ctx) throws Exception {
    while (runningChecker.isRunning()) {
      exceptionListener.checkErroneous();
      Long messageId = queue.take();
      MQTTMessage message = store.retrieve(messageId);
      synchronized (ctx.getCheckpointLock()) {
        if (!autoAck && addId(messageId)) {
          ctx.collect(message);
          //MQTT5 ack ???
          // unacknowledgedMessages.put(messageId);
        } else {
          ctx.collect(message);
        }
      }
    }
  }

  @Override
  public void cancel() {
    runningChecker.setIsRunning(false);
  }

  @Override
  public TypeInformation<MQTTMessage> getProducedType() {
    //return objectSerializer.getProducedType();
    return TypeInformation.of(MQTTMessage.class);
  }

  @Override
  protected void acknowledgeIDs(long checkpointId, Set<Long> uIds) {
    try {
      for (Long messageId : uIds) {
        store.remove(messageId);
      }
    } catch (Exception e) {
      if (logFailuresOnly) {
        LOG.error("Failed to acknowledge MQTT message");
      } else {
        throw new RuntimeException("Failed to acknowledge MQTT message");
      }
    }
  }

}
