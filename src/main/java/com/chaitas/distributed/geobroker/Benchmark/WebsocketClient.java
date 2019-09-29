package com.chaitas.distributed.geobroker.Benchmark;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Utils.JSONable;
import org.java_websocket.client.WebSocketClient;

import org.java_websocket.handshake.ServerHandshake;

public class WebsocketClient extends WebSocketClient {

    private final List<ExternalMessage> responses = new ArrayList<>();
    private Integer responsesSize = 0;

    public WebsocketClient(URI serverURI ) {
        super( serverURI );
    }

    @Override
    public void onOpen( ServerHandshake handshakedata ) {
        System.out.println( "Opened connection" );
    }

    @Override
    public void onMessage( String message ) {
        try {
            Optional<ExternalMessage> message0 = JSONable.fromJSON(message, ExternalMessage.class);
            ExternalMessage externalMessage = message0.get();
            System.out.println("Received message : " + externalMessage);
            synchronized (responses) {
                responses.add(externalMessage);
                responses.notifyAll();
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

    public ExternalMessage sendAndReceive(byte[] data, long timeoutMillis) throws InterruptedException {
        this.send(data);
        synchronized (responses){
            while (responses.size() == 0 || responsesSize >= responses.size()) responses.wait(timeoutMillis);
            synchronized (responsesSize) {
                responsesSize++;
            }
            return responses.get(responses.size() - 1);
        }
    }

    public static void main( String[] args ) throws URISyntaxException {
        WebsocketClient c = new WebsocketClient( new URI( "ws://localhost:8000/api" ));
        c.connect();
        c.close();
    }

}