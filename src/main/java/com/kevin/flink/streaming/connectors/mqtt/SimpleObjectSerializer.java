package com.kevin.flink.streaming.connectors.mqtt;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * author: kevin4936@163.com
 *
 *
 */
public class SimpleObjectSerializer<T> implements SerializationSchema<T>, DeserializationSchema<T>, Serializer<T> {

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
