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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * author: kevin4936@163.com
 * 
 * 
 */

public class MQTTMessage implements Serializable {

  private String topic;
  private int messageId;
  private boolean mutable = true;
  private boolean retained = false;
  private boolean duplicate = false;
  private int qos = 1;
  private byte[] payload;

  public boolean isMutable() {
    return mutable;
  }

  public void setMutable(boolean mutable) {
    this.mutable = mutable;
  }

  public byte[] getPayload() {
    return payload;
  }

  public void setPayload(byte[] payload) {
    this.payload = payload;
  }

  public int getQos() {
    return qos;
  }

  public void setQos(int qos) {
    this.qos = qos;
  }

  public boolean isRetained() {
    return retained;
  }

  public void setRetained(boolean retained) {
    this.retained = retained;
  }

  public boolean isDuplicate() {
    return duplicate;
  }

  public void setDuplicate(boolean duplicate) {
    this.duplicate = duplicate;
  }

  public int getMessageId() {
    return messageId;
  }

  public void setMessageId(int messageId) {
    this.messageId = messageId;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getTopic() {
    return this.topic;
  }

  @Override
  public String toString() {
    String format = "MQTTMessage.\r\n" +
        "|Topic: %s\r\n" +
        "|MessageID: %s\r\n" +
        "|QoS: %s\r\n" +
        "|Payload as string: %s\r\n" +
        "|isRetained: %s\r\n" +
        "|isDuplicate: %s\r\n" +
        "|TimeStamp: %s\r\n";
    Timestamp timestamp = Timestamp.valueOf(MQTTStreamConstants.DATE_FORMAT.format(Calendar.getInstance().getTime()));
    return String.format(format, topic, messageId, qos, new String(payload, Charset.defaultCharset()), retained, duplicate,
        timestamp);
  }
}