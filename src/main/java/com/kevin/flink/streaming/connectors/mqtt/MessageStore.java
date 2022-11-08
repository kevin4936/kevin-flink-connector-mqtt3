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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttPersistable;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

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

class MqttPersistableData implements MqttPersistable {

    private byte[] bytes;

    public MqttPersistableData(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public int getHeaderLength() {
        return bytes.length;
    }

    @Override
    public int getHeaderOffset() {
        return 0;
    }

    @Override
    public int getPayloadOffset() {
        return 0;
    }

    @Override
    public byte[] getPayloadBytes() {
        return null;
    }

    @Override
    public byte[] getHeaderBytes() {
        return bytes;
    }

    @Override
    public int getPayloadLength() {
        return 0;
    }
}

interface Serializer<T> {

    T deserialize(byte[] x);

    byte[] serialize(T x);
}

class SimpleObjectSerializer<T> implements SerializationSchema<T>, DeserializationSchema<T>, Serializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleObjectSerializer.class);

    @Override
    public T deserialize(byte[] x) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(x);
                ObjectInputStream in = new ObjectInputStream(bis);) {
            Object obj = in.readObject();
            in.close();
            return (T) obj;
        } catch (Throwable t) {
            LOG.warn("failed to close stream", t);
        }
        return null;
    }

    @Override
    public byte[] serialize(T x) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos);) {
            out.writeObject(x);
            out.flush();
            byte[] bytes = bos.toByteArray();
            bos.close();
            return bytes;
        } catch (Throwable t) {
            LOG.warn("failed to close stream", t);
        }
        return null;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(new TypeHint<T>(){});
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }
}

/**
 * A message store to persist messages received. This is not intended to be
 * thread safe.
 * It uses `MqttDefaultFilePersistence` for storing messages on disk locally on
 * the client.
 */

class LocalMessageStore<T> implements MessageStore<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalMessageStore.class);

    private MqttClientPersistence persistentStore = null;
    // Serialization scheme that is used to convert input message to bytes
    private final SerializationSchema<T> serializationSchema;
    // Deserialization scheme that is used to convert bytes to output message
    private final DeserializationSchema<T> deserializationSchema;

    public LocalMessageStore(MqttClientPersistence persistentStore, SerializationSchema<T> serializationSchema,
            DeserializationSchema<T> deserializationSchema) {
        this.persistentStore = persistentStore;
        this.serializationSchema = serializationSchema;
        this.deserializationSchema = deserializationSchema;
    }

    private byte[] get(Long id) {
        try {
            MqttPersistable mqttPersistableData = persistentStore.get(id.toString());
            return mqttPersistableData.getHeaderBytes();
        } catch (MqttPersistenceException e) {
            LOG.error("MQTT LocalMessageStore get error: ", e);
        }
        return null;
    }

    @Override
    public Long maxProcessedOffset() {
        List<Long> ids = new ArrayList<>();
        try {
            Enumeration<String> keys = persistentStore.keys();
            while (keys.hasMoreElements()) {
                ids.add(Long.valueOf(keys.nextElement()));
            }
        } catch (MqttPersistenceException e) {
            LOG.error("MQTT LocalMessageStore maxProcessedOffset error: ", e);
        }
        return Collections.max(ids);
    }

    /** Store a single id and corresponding serialized message */
    @Override
    public Boolean store(Long id, T message) {
        byte[] bytes = serializationSchema.serialize(message);
        try {
            persistentStore.put(id.toString(), new MqttPersistableData(bytes));
            return true;
        } catch (MqttPersistenceException e) {
             LOG.warn(String.format("Failed to store message Id: %s", id), e);
            return false;
        }
    }

    /** Retrieve message corresponding to a given id. */
    @Override
    public T retrieve(Long id) throws Exception {
        return deserializationSchema.deserialize(get(id));
    }

    @Override
    public void remove(Long id) {
        try {
            persistentStore.remove(id.toString());
        } catch (MqttPersistenceException e) {
            LOG.error("MQTT LocalMessageStore remove error: ", e);
        }
    }

    @Override
    public void close() throws MqttPersistenceException {
        persistentStore.close();
    }

}
