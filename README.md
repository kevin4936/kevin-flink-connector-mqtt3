<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
# Flink SQL Streaming MQTT Data Source

A library for writing and reading data from MQTT Servers using Flink SQL Streaming (or Structured streaming).

This library comes from https://github.com/apache/bahir/tree/master/sql-streaming-mqtt  .

## Linking

Using SBT:

    libraryDependencies += "com.kevin.bahir" %% "kevin-flink-connector-mqtt3" % "{{site.FLINK_VERSION}}"

Using Maven:

    <dependency>
        <groupId>com.kevin.bahir</groupId>
        <artifactId>kevin-flink-connector-mqtt3_{{site.SCALA_BINARY_VERSION}}</artifactId>
        <version>{{site.FLINK_VERSION}}</version>
    </dependency>

mvn clean install -DskipTests  -Denforcer.skip=true


Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.

This library is cross-published for Scala 2.12 and Scala 2.13, so users should replace the proper Scala version in the commands listed above.

## Examples

Please see `src/test/java/com/kevin/flink/streaming/connectors/mqtt/MQTTStreamSourceAndSinkTest.java` for full example.

## Configuration options

This connector uses [Eclipse Paho Java Client](https://eclipse.org/paho/clients/java/). Client API documentation is located [here](http://www.eclipse.org/paho/files/javadoc/index.html).

| Parameter name             | Description                                                                                                                                                                                                                                                                                       | Eclipse Paho reference                                                   |
|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| `brokerUrl`                | URL MQTT client connects to. Specify this parameter or _path_. Example: _tcp://localhost:1883_, _ssl://localhost:1883_.                                                                                                                                                                           |                                                                          |
| `persistence`              | Defines how incoming messages are stored. If _memory_ is provided as value for this option, recovery on restart is not supported. Otherwise messages are stored on disk and parameter _localStorage_ may define target directory.                                                                 |                                                                          |
| `topic`                    | Topic which client subscribes to.                                                                                                                                                                                                                                                                 |                                                                          |
| `clientId`                 | Uniquely identifies client instance. Provide the same value to recover a stopped source client. MQTT sink ignores client identifier, because Flink batch can be distributed across multiple workers whereas MQTT broker does not allow simultaneous connections with same ID from multiple hosts. |                                                                          |
| `QoS`                      | The maximum quality of service to subscribe each topic at. Messages published at a lower quality of service will be received at the published QoS. Messages published at a higher quality of service will be received using the QoS specified on the subscribe.                                   |                                                                          |
| `username`                 | User name used to authenticate with MQTT server. Do not set it, if server does not require authentication. Leaving empty may lead to errors.                                                                                                                                                      | `MqttConnectOptions.setUserName`                                         |
| `password`                 | User password.                                                                                                                                                                                                                                                                                    | `MqttConnectOptions.setPassword`                                         |
| `cleanSession`             | Setting to _true_ starts a clean session, removes all check-pointed messages persisted during previous run. Defaults to `false`.                                                                                                                                                                  | `MqttConnectOptions.setCleanSession`                                     |
| `connectionTimeout`        | Sets the connection timeout, a value of _0_ is interpreted as wait until client connects.                                                                                                                                                                                                         | `MqttConnectOptions.setConnectionTimeout`                                |
| `keepAlive`                | Sets the "keep alive" interval in seconds.                                                                                                                                                                                                                                                        | `MqttConnectOptions.setKeepAliveInterval`                                |
| `mqttVersion`              | Specify MQTT protocol version.                                                                                                                                                                                                                                                                    | `MqttConnectOptions.setMqttVersion`                                      |
| `maxInflight`              | Sets the maximum inflight requests. Useful for high volume traffic.                                                                                                                                                                                                                               | `MqttConnectOptions.setMaxInflight`                                      |
| `autoReconnect`            | Sets whether the client will automatically attempt to reconnect to the server upon connectivity disruption.                                                                                                                                                                                       | `MqttConnectOptions.setAutomaticReconnect`                               |
| `ssl.protocol`             | SSL protocol. Example: _SSLv3_, _TLS_, _TLSv1_, _TLSv1.2_.                                                                                                                                                                                                                                        | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.protocol`            |
| `ssl.key.store`            | Absolute path to key store file.                                                                                                                                                                                                                                                                  | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStore`            |
| `ssl.key.store.password`   | Key store password.                                                                                                                                                                                                                                                                               | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStorePassword`    |
| `ssl.key.store.type`       | Key store type. Example: _JKS_, _JCEKS_, _PKCS12_.                                                                                                                                                                                                                                                | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStoreType`        |
| `ssl.key.store.provider`   | Key store provider. Example: _IBMJCE_.                                                                                                                                                                                                                                                            | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStoreProvider`    |
| `ssl.trust.store`          | Absolute path to trust store file.                                                                                                                                                                                                                                                                | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStore`          |
| `ssl.trust.store.password` | Trust store password.                                                                                                                                                                                                                                                                             | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStorePassword`  |
| `ssl.trust.store.type`     | Trust store type. Example: _JKS_, _JCEKS_, _PKCS12_.                                                                                                                                                                                                                                              | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStoreType`      |
| `ssl.trust.store.provider` | Trust store provider. Example: _IBMJCEFIPS_.                                                                                                                                                                                                                                                      | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStoreProvider`  |
| `ssl.ciphers`              | List of enabled cipher suites. Example: _SSL_RSA_WITH_AES_128_CBC_SHA_.                                                                                                                                                                                                                           | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.enabledCipherSuites` |

## Environment variables

Custom environment variables allowing to manage MQTT connectivity performed by sink connector:

 * `spark.mqtt.client.connect.attempts` Number of attempts sink will try to connect to MQTT broker before failing.
 * `spark.mqtt.client.connect.backoff` Delay in milliseconds to wait before retrying connection to the server.
 * `spark.mqtt.connection.cache.timeout` Sink connector caches MQTT connections. Idle connections will be closed after timeout milliseconds.
 * `spark.mqtt.client.publish.attempts` Number of attempts to publish the message before failing the task.
 * `spark.mqtt.client.publish.backoff` Delay in milliseconds to wait before retrying send operation.

## Best Practices.

1. Turn Mqtt into a more reliable messaging service.

> *MQTT is a machine-to-machine (M2M)/"Internet of Things" connectivity protocol. It was designed as an extremely lightweight publish/subscribe messaging transport.*

The design of Mqtt and the purpose it serves goes well together, but often in an application it is of utmost value to have reliability. Since mqtt is not a distributed message queue and thus does not offer the highest level of reliability features. It should be redirected via a kafka message queue to take advantage of a distributed message queue. In fact, using a kafka message queue offers a lot of possibilities including a single kafka topic subscribed to several mqtt sources and even a single mqtt stream publishing to multiple kafka topics. Kafka is a reliable and scalable message queue.

2. What is the solution for a situation when there are a large number of varied mqtt sources, each with different schema and throughput characteristics.

Generally, one would create a lot of streaming pipelines to solve this problem. This would either require a very sophisticated scheduling setup or will waste a lot of resources, as it is not certain which stream is using more amount of data.

The general solution is both less optimum and is more cumbersome to operate, with multiple moving parts incurs a high maintenance overall. As an alternative, in this situation, one can setup a single topic kafka-spark stream, where message from each of the varied stream contains a unique tag separating one from other streams. This way at the processing end, one can distinguish the message from one another and apply the right kind of decoding and processing. Similarly while storing, each message can be distinguished from others by a tag that distinguishes.
