package com.chaitas.distributed.geobroker.Benchmark;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.net.URISyntaxException;

public class SocketClient {

    public interface SocketListener {
        void onOpen(ServerHandshake serverHandshake);
        void onMessage(String s);
        void onClose(int i, String s, boolean b);
    }

    SocketListener listener;

    public SocketClient(SocketListener l) {
        listener = l;
    }

    private WebSocketClient mWebSocketClient;

    private void connectWebSocket(String url) throws URISyntaxException {
        URI uri;
        uri = new URI(url);

        mWebSocketClient = new WebSocketClient(uri) {
            @Override
            public void onOpen(ServerHandshake serverHandshake) {
                System.out.println("Websocket Opened");
                mWebSocketClient.send("Hello ... ");
                listener.onOpen(serverHandshake);
            }

            @Override
            public void onMessage(String s) {
                final String message = s;
                // Here i want to use callback
                listener.onMessage(s);
            }

            @Override
            public void onClose(int i, String s, boolean b) {
                System.out.println("Websocket Closed");
                listener.onClose(i, s, b);
            }

            @Override
            public void onError(Exception e) {
                System.out.println("Websocket Error" + e.getMessage());
            }
        };
        mWebSocketClient.connect();
    }
}