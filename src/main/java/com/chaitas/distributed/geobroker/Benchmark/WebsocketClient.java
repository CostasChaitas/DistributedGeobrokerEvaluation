package com.chaitas.distributed.geobroker.Benchmark;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.CONNECTPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Location;
import com.chaitas.distributed.geobroker.Utils.JSONable;
import com.chaitas.distributed.geobroker.Utils.KryoSerializerPool;
import org.java_websocket.client.WebSocketClient;

import org.java_websocket.handshake.ServerHandshake;

public class WebsocketClient extends WebSocketClient {

    private final String clientName;
    private Long initialTime;
    private final KryoSerializerPool kryo = new KryoSerializerPool();

    public WebsocketClient(URI serverURI, String clientName) {
        super( serverURI );
        this.clientName = clientName;
        this.setConnectionLostTimeout(0);
    }

    @Override
    public void onOpen( ServerHandshake handshakedata ) {
        System.out.println("Websocket connection opened: " + this.clientName);
    }

    @Override
    public void onMessage( String message ) {
        try {
            Optional<ExternalMessage> message0 = JSONable.fromJSON(message, ExternalMessage.class);
            ExternalMessage externalMessage = message0.get();
            if(externalMessage.getControlPacketType() == ControlPacketType.CONNACK) {
                return;
            }
            long receivedTime = System.currentTimeMillis() - initialTime;
            System.out.println(clientName + " received message : " + externalMessage.getControlPacketType().toString());
            if(externalMessage.getControlPacketType() == ControlPacketType.PUBLISH) {
                BenchmarkHelper.addEntry("PUBLISH_RECEIVED", externalMessage.getId(), externalMessage.getClientIdentifier(), receivedTime);
            } else{
                BenchmarkHelper.addEntry(externalMessage.getControlPacketType().toString(), externalMessage.getId(), this.clientName, receivedTime);
            }
        }catch (Exception e) {
            System.out.println("Cannot deserialize received message");
            throw new Error(e);
        }
    }

    @Override
    public void onClose( int code, String reason, boolean remote ) {
        System.out.println( "Connection closed by " + ( remote ? "remote peer" : "us" ) + " Code: " + code + " Reason: " + reason );
    }

    public void sendMessage(ExternalMessage message){
        byte[] arr = kryo.write(message);
        Long timeNowMillis = System.currentTimeMillis() - initialTime;
        BenchmarkHelper.addEntry(message.getControlPacketType().toString(), message.getId(), clientName, timeNowMillis);
        this.send(arr);
        System.out.println(clientName + " sending message : " + message.getControlPacketType().toString());
    }

    public void connectClientWebsocket() {
        Location location = Location.random();
        ExternalMessage connect = new ExternalMessage(UUID.randomUUID().toString(), clientName, ControlPacketType.CONNECT, new CONNECTPayload(location));
        byte[] connectMsg = kryo.write(connect);
        this.send(connectMsg);
    }

    @Override
    public void onError( Exception ex ) {
        ex.printStackTrace();
    }

    public String getClientName() {
        return clientName;
    }

    public Long getTime() {
        return initialTime;
    }

    public void setTime(Long initialTime) {
        this.initialTime = initialTime;
    }

    public static void main( String[] args ) throws URISyntaxException {
        WebsocketClient c = new WebsocketClient( new URI( "ws://localhost:8000/api"), "testClient");
        c.connect();
        c.close();
    }

}