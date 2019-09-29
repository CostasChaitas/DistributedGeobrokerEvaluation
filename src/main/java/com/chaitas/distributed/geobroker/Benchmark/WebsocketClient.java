package com.chaitas.distributed.geobroker.Benchmark;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Utils.JSONable;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft;

import org.java_websocket.handshake.ServerHandshake;

public class WebsocketClient extends WebSocketClient {

    public WebsocketClient(URI serverUri , Draft draft ) {
        super( serverUri, draft );
    }

    public WebsocketClient(URI serverURI ) {
        super( serverURI );
    }

    public WebsocketClient(URI serverUri, Map<String, String> httpHeaders ) {
        super(serverUri, httpHeaders);
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
            BenchmarkClient.latch.countDown();
        } catch (Exception e) {
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

    public static void main( String[] args ) throws URISyntaxException {
        WebsocketClient c = new WebsocketClient( new URI( "ws://localhost:8000/api" ));
        c.connect();
        c.close();
    }

}