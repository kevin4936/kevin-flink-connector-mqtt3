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

package com.kevin.flink.streaming.connectors.mqtt.internal;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;

import java.io.Serializable;

/**
 * author: kevin4936@163.com
 * 
 * 
 */

public class MQTTExceptionListener implements Serializable {

    private static final long serialVersionUID = 1234567890L;

    private final boolean logFailuresOnly;
    private final Logger logger;
    private MqttException exception;

    public MQTTExceptionListener(Logger logger, boolean logFailuresOnly) {
        this.logger = logger;
        this.logFailuresOnly = logFailuresOnly;
    }

    public void onException(MqttException e) {
        this.exception = e;
    }

    /**
     * Check if the listener received an asynchronous exception. Throws an exception
     * if it was
     * received and if logFailuresOnly was set to true. Resets the state after the
     * call
     * so a single exception can be thrown only once.
     *
     * @throws MqttException if exception was received and logFailuresOnly was set to
     *                      true.
     */
    public void checkErroneous() throws MqttException {
        if (exception == null) {
            return;
        }

        MqttException recordedException = exception;
        exception = null;
        if (logFailuresOnly) {
            logger.error("Received MQTT exception", recordedException);
        } else {
            throw recordedException;
        }
    }
}
