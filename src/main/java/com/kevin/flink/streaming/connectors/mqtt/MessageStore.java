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

import org.eclipse.paho.client.mqttv3.MqttPersistenceException;

/**
 * author: kevin4936@163.com
 * 
 * 
 */

/** A message store for MQTT stream source for SQL Streaming. */
public interface MessageStore<T> {

    /** Store a single id and corresponding serialized message */
    Boolean store(Long id, T message);

    /** Retrieve message corresponding to a given id. */
    T retrieve(Long id) throws Exception;

    /** Highest offset we have stored */
    Long maxProcessedOffset();

    /** Remove message corresponding to a given id. */
    void remove(Long id);

    void close() throws MqttPersistenceException;

}





