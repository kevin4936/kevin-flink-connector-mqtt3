package com.kevin.flink.streaming.connectors.mqtt;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttPersistable;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * author: kevin4936@163.com
 *
 *
 */


/**
 * A message store to persist messages received. This is not intended to be
 * thread safe.
 * It uses `MqttDefaultFilePersistence` for storing messages on disk locally on
 * the client.
 */
public class LocalMessageStore<T> implements MessageStore<T> {

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
