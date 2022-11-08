package com.kevin.flink.streaming.connectors.mqtt;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttWireMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
public class IOTMachine {

    private static final Logger LOG = LoggerFactory.getLogger(IOTMachine.class);
    
    private static final String USER_CONTEXT = "flink agent mqtt";
    public static MqttAsyncClient mqttAsyncClient = null;
    private static MemoryPersistence memoryPersistence = null;
    private static MqttConnectOptions mqttConnectOptions = null;

    private static IOTMachine instance = null;

    private String serverURI = "";
    private String clientId = "";
    private String userName = "";
    private String password = "";
    private int qos = 0;

    private Set<String> topicSet = new HashSet<>();

    public static IOTMachine getInstance() throws Exception {
        if (instance == null) {
            synchronized (IOTMachine.class) {
                if (instance == null) {
                    instance = new IOTMachine();
                }
            }
        }
        return instance;
    }

    private IOTMachine(){
    }

    public void setConfig(String serverURI, String clientId, String userName, String password, int qos){
        this.serverURI = serverURI;
        this.clientId = clientId;
        this.userName = userName;
        this.password = password;
        this.qos = qos;
        checkParams(serverURI, clientId, userName, password, qos);
    }

    private boolean checkParams(String serverURI, String clientId, String userName, String password, int qos){
        if(StringUtils.isEmpty(serverURI)){
            LOG.warn("MQTT config serverURI is null");
            return false;
        }
        if(StringUtils.isEmpty(clientId)){
            LOG.warn("MQTT config clientId is null");
            return false;
        }
        if(StringUtils.isEmpty(userName)){
            LOG.warn("MQTT config userName is null");
            return false;
        }
        if(StringUtils.isEmpty(password)){
            LOG.warn("MQTT config password is null");
            return false;
        }
        if(qos < 0 || qos > 3){
            LOG.warn("MQTT config qos must be 0, 1, 2, 3");
            return false;
        }
        return true;
    }

    public void init(IMqttCallbackListener callback) {
        if(!checkParams(serverURI, clientId, userName, password, qos)){
            return;
        }
        mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setCleanSession(true);
        mqttConnectOptions.setConnectionTimeout(30);
        mqttConnectOptions.setKeepAliveInterval(20);
        mqttConnectOptions.setAutomaticReconnect(true);
        mqttConnectOptions.setUserName(userName);
        mqttConnectOptions.setPassword(password.toCharArray());
        memoryPersistence = new MemoryPersistence();
        try {
            mqttAsyncClient = new MqttAsyncClient(serverURI, clientId, memoryPersistence);
            mqttAsyncClient.setCallback(new MqttCallbackExtended() {
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    LOG.info("MQTT connect complete.");
                    subscribeAllTopic();
                }

                @Override
                public void connectionLost(Throwable cause) {
                    LOG.warn("Connection to mqtt server lost.", cause);
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    LOG.info("Received message on topic: " + topic + " -> " + new String(message.getPayload()));
                    callback.onSuccess(message);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {

                }
            });

        } catch (MqttException e) {
            LOG.error("MQTT client create failure, MqttException:" + e.getMessage());
        }
        if(null != mqttAsyncClient) {
            if(!mqttAsyncClient.isConnected()) {
                connect();
            }
        }else {
            LOG.info("MQTT connect failure，mqtt client is null");
        }
    }

    private void connect(){
        if(!checkParams(serverURI, clientId, userName, password, qos)){
            return;
        }

        try {
            mqttAsyncClient.connect(mqttConnectOptions, USER_CONTEXT, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken iMqttToken) {
                    LOG.info("MQTT connect success, Message ID: " + iMqttToken.getMessageId());
                }

                @Override
                public void onFailure(IMqttToken iMqttToken, Throwable throwable) {
                    LOG.error("MQTT connect failure, exception:" + throwable.getMessage());
                }
            });
        } catch (MqttException e) {
            LOG.error("MQTT connect failure, exception:" + e.getMessage());
        }
    }

    public void closeConnect() {
        if(!checkParams(serverURI, clientId, userName, password, qos)){
            return;
        }

        if(null != memoryPersistence) {
            try {
                memoryPersistence.close();
            } catch (MqttPersistenceException e) {
                LOG.error("MQTT persistence close failure, Exception:" + e.getMessage());
            }
        }else {
            LOG.info("MQTT persistence close failure, persistence is null");
        }

        if(null != mqttAsyncClient) {
            if(mqttAsyncClient.isConnected()) {
                try {
                    mqttAsyncClient.disconnect(USER_CONTEXT, new IMqttActionListener() {
                        @Override
                        public void onSuccess(IMqttToken asyncActionToken) {
                            LOG.info("MQTT disconnect success");
                        }

                        @Override
                        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                            LOG.error("MQTT disconnect failure, Exception：" + exception.getMessage());
                        }
                    });
                    mqttAsyncClient.close();
                } catch (MqttException e) {
                    LOG.error("MQTT disconnect failure, Exception：" + e.getMessage());
                }
            }else {
                LOG.info("MQTT disconnect failure, mqtt client is not connect");
            }

            mqttAsyncClient = null;
        }else {
            LOG.info("MQTT disconnect failure, mqtt client is null");
        }
    }

    public void publishMessage(String pubTopic, String message) {
        publishMessage(pubTopic, message, qos);
    }

    public void publishMessage(String pubTopic, String message, int qos) {
        if(!checkParams(serverURI, clientId, userName, password, qos)){
            return;
        }
        if(null== mqttAsyncClient || !mqttAsyncClient.isConnected()) {
            reConnect();
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if(null!= mqttAsyncClient && mqttAsyncClient.isConnected()) {
            MqttMessage mqttMessage = new MqttMessage();
            mqttMessage.setQos(qos);
            mqttMessage.setPayload(message.getBytes());
            try {
                IMqttDeliveryToken token = mqttAsyncClient.publish(pubTopic, mqttMessage, USER_CONTEXT, new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken asyncActionToken) {
                        LOG.info("MQTT client publish message success, Message ID: " + asyncActionToken.getMessageId());
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                        LOG.error("MQTT client publish message failure，Exception：" + exception.getMessage());
                    }
                });
                token.waitForCompletion();
                LOG.info("message is published completely! " + token.isComplete());
                LOG.info("messageId:" + token.getMessageId());
                MqttWireMessage response = token.getResponse();
                if(response!=null){
                    LOG.info("response: " + new String(response.getPayload()));
                }
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public void reConnect() {
        if(!checkParams(serverURI, clientId, userName, password, qos)){
            return;
        }
        if(null != mqttAsyncClient) {
            if(!mqttAsyncClient.isConnected()) {
                if(null != mqttConnectOptions) {
                    try {
                        mqttAsyncClient.reconnect();
                    } catch (MqttException e) {
                        LOG.error("MQTT reconnect failure, MqttException：" + e.getMessage());
                    }
                }else {
                    LOG.info("MQTT reconnect failure, mqttConnectOptions is null");
                }
            }else {
                LOG.info("MQTT reconnect failure, mqtt client is null or connect");
            }
        }
    }

    private void subscribeAllTopic(){
        for(String topic: topicSet){
            subTopic(topic);
        }
    }

    public void subscribeTopic(String topic) {
        topicSet.add(topic);
    }

    private void subTopic(String topic) {
        if(!checkParams(serverURI, clientId, userName, password, qos)){
            return;
        }

        if(null != mqttAsyncClient && mqttAsyncClient.isConnected()) {
            try {
                mqttAsyncClient.subscribe(topic, qos, USER_CONTEXT, new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken asyncActionToken) {
                        LOG.info(String.format("MQTT subscribe success topic=%s", topic));
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                        LOG.error(String.format("MQTT subscribe failure topic=%s", topic) + ", exception：" + exception.getMessage());
                    }
                });
            } catch (MqttException e) {
                LOG.error(String.format("MQTT ubscribe %s failure, ", topic) + "Exception：" + e.getMessage());
            }
        }else {
            LOG.info(String.format("MQTT subscribe %s failure, mqtt client is error", topic));
        }
    }


    public void unsubscribeTopic(String topic) {
        if(!checkParams(serverURI, clientId, userName, password, qos)){
            return;
        }

        if(null != mqttAsyncClient && !mqttAsyncClient.isConnected()) {
            try {
                mqttAsyncClient.unsubscribe(topic, USER_CONTEXT, new IMqttActionListener() {
                    @Override
                    public void onSuccess(IMqttToken asyncActionToken) {
                        LOG.info("MQTT unsubscribe success");
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                        LOG.error(String.format("MQTT unsubscribe  %s  failure：", topic) + exception.getMessage());
                    }
                });
            } catch (MqttException e) {
                LOG.error(String.format("MQTT unsubscribe %s exception：", topic) + e.getMessage());
            }
        }else {
            LOG.info(String.format("MQTT unsubscribe %s failure, mqtt client is error", topic));
        }
    }

//    public static void main(String [] args) throws Exception {
//        IOTMachine mqttClient = IOTMachine.getInstance();
//        mqttClient.subscribeTopic("flink-mqtt-callback");
//        //mqttClient.setConfig("tcp://127.0.0.1:1883", "testClient", "test2", "test_password", 2);
//        mqttClient.setConfig("tcp://test.mosquitto.org", "testClient", "test2", "test_password", 2);
//        mqttClient.init();
//        while (true) {
//            try {
//                String message = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.now());
//                mqttClient.publishMessage("test", "time-" + message);
//                Thread.sleep(3000);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//    }

}

interface IMqttCallbackListener {

    void onSuccess(MqttMessage message);

    void onFailure(MqttMessage message, Throwable exception);
}

