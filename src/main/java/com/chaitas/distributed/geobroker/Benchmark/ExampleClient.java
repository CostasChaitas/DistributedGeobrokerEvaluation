package com.chaitas.distributed.geobroker.Benchmark;

import java.util.concurrent.ExecutionException;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;

import static org.asynchttpclient.Dsl.*;

public class ExampleClient {

    private final String apiURL;
    private final String clientName;
    public WebSocket websocket;

    public ExampleClient(String apiURL, String clientName) {
        this.apiURL = apiURL;
        this.clientName = clientName;
        this.websocketListener();
    }

    private void websocketListener() {
        try {
            AsyncHttpClient c = asyncHttpClient();
            websocket = c.prepareGet(this.apiURL)
                    .execute(
                            new WebSocketUpgradeHandler.Builder()
                                    .addWebSocketListener(new WebSocketListener() {
                                        @Override
                                        public void onOpen(WebSocket websocket) {
                                            System.out.println("Connection opened...");
                                        }

                                        @Override
                                        public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                                            System.out.println("Received message");
                                            System.out.println(payload);
                                            //latch.countDown();
                                        }

                                        @Override
                                        public void onError(Throwable t) {
                                            throw new Error(t);
                                        }

                                        @Override
                                        public void onClose(WebSocket webSocket, int i, String s) {
                                            System.out.println("Closed Websocket");
                                        }


                                    }).build()).get();
        } catch (ExecutionException | InterruptedException ex) {
            System.out.println(ex);
        }
    }

    public String getApiURL() {
        return apiURL;
    }

    public String getClientName() {
        return clientName;
    }
}