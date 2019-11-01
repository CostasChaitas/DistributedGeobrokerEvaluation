package com.chaitas.distributed.geobroker.Benchmark;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Utils.JSONable;
import org.java_websocket.client.WebSocketClient;

import org.java_websocket.handshake.ServerHandshake;

public class WebsocketClient extends WebSocketClient {

    private final String clientName;
    private Long time;

    public WebsocketClient(URI serverURI, String clientName) {
        super( serverURI );
        this.clientName = clientName;
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

            long receivedTime = System.currentTimeMillis() - time;
            System.out.println(clientName + " received message : " + externalMessage.getControlPacketType().toString());
            if(externalMessage.getControlPacketType() == ControlPacketType.PUBLISH) {
                BenchmarkHelper.addEntry("PUBLISH_RECEIVED", this.clientName, receivedTime);
            } else{
                BenchmarkHelper.addEntry(externalMessage.getControlPacketType().toString(), this.clientName, receivedTime);
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

    @Override
    public void onError( Exception ex ) {
        ex.printStackTrace();
    }

    public String getClientName() {
        return clientName;
    }
    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public static void main( String[] args ) throws URISyntaxException {
        WebsocketClient c = new WebsocketClient( new URI( "ws://localhost:8000/api"), "testClient");
        c.connect();
        c.close();
    }

}