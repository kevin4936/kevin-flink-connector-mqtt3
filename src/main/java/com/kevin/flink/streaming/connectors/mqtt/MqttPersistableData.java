package com.kevin.flink.streaming.connectors.mqtt;

import org.eclipse.paho.client.mqttv3.MqttPersistable;

/**
 * author: kevin4936@163.com
 *
 *
 */
public class MqttPersistableData implements MqttPersistable {

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
