package com.kevin.flink.streaming.connectors.mqtt;

/**
 * author: kevin4936@163.com
 *
 *
 */
public interface Serializer<T> {

    T deserialize(byte[] x);

    byte[] serialize(T x);
}