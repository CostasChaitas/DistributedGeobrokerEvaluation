package com.chaitas.distributed.geobroker.Benchmark;

import java.util.concurrent.ExecutionException;


import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.CONNECTPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.SUBSCRIBEPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Geofence;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Location;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Topic;
import com.chaitas.distributed.geobroker.Utils.KryoSerializerPool;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;

import static org.asynchttpclient.Dsl.*;

public class ExampleClient {

    public WebSocket websocket;
    public String clientName;
    private static KryoSerializerPool kryo = new KryoSerializerPool();

    public ExampleClient(String serverURI, String clientName) {
        this.clientName = clientName;
        try {
            AsyncHttpClient c = asyncHttpClient();
            websocket = c.prepareGet(serverURI)
                    .execute(
                            new WebSocketUpgradeHandler.Builder()
                                    .addWebSocketListener(new WebSocketListener() {
                                        @Override
                                        public void onOpen(WebSocket websocket) {
                                            System.out.println("Connection opened...");
                                            websocket.sendTextFrame("Hello...");
                                        }

                                        @Override
                                        public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                                            System.out.println("Received message");
                                            System.out.println(payload);
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

    public String getClientName() {
        return clientName;
    }

    public static void main (String[] args) {
        ExampleClient exampleClient = new ExampleClient("ws://127.0.0.1:8000/api", "test");
        Location location = Location.random();
        ExternalMessage connect = new ExternalMessage("test12", ControlPacketType.CONNECT, new CONNECTPayload(location));
        byte[] connectMsg = kryo.write(connect);
        exampleClient.websocket.sendBinaryFrame(connectMsg);

        ExternalMessage subscribe = new ExternalMessage("test12", ControlPacketType.SUBSCRIBE,
                    new SUBSCRIBEPayload(new Topic("testTopic"), Geofence.circle(location, 0.0)));
        byte[] subscribeMsg = kryo.write(subscribe);
        exampleClient.websocket.sendBinaryFrame(subscribeMsg);

    }

}