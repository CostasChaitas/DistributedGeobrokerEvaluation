package com.chaitas.distributed.geobroker.Benchmark;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Utils.JSONable;
import org.java_websocket.client.WebSocketClient;

import org.java_websocket.handshake.ServerHandshake;

public class WebsocketClient extends WebSocketClient {

    private final String writeFilePath;
    private final BenchmarkHelper benchmarkHelper;

    public WebsocketClient(URI serverURI, BenchmarkHelper benchmarkHelper, String writeFilePath ) {
        super( serverURI );
        this.writeFilePath = writeFilePath;
        this.benchmarkHelper = benchmarkHelper;
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
            createStringFromReceivedMessage(externalMessage);
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


    private void createStringFromReceivedMessage(ExternalMessage message) {
        ControlPacketType controlPacketType = message.getControlPacketType();
        long time = System.nanoTime();
        switch (controlPacketType) {
            case CONNACK:
                String connackText = String.format("%2d;%s;;;;;\n", time, "CONNACK");
                benchmarkHelper.addEntry(writeFilePath, connackText);
                break;
            case PINGRESP:
                String pingrespText = String.format("%2d;%s;;;;;\n", time, "PINGRESP");
                benchmarkHelper.addEntry(writeFilePath, pingrespText);
                break;
            case SUBACK:
                String subackText = String.format("%2d;%s;;;;;\n", time, "SUBACK");
                benchmarkHelper.addEntry(writeFilePath, subackText);
                break;
            case PUBACK:
                String pubackText = String.format("%2d;%s;;;;;\n", time, "PUBACK");
                benchmarkHelper.addEntry(writeFilePath, pubackText);
                break;
            case PUBLISH:
                String publishText = String.format("%2d;%s;;;;;\n", time, "PUBLISH");
                benchmarkHelper.addEntry(writeFilePath, publishText);
                break;
            case INCOMPATIBLEPayload:
                String incompatibleText = String.format("%2d;%s;;;;;\n", time, "INCOMPATIBLEPayload");
                benchmarkHelper.addEntry(writeFilePath, incompatibleText);
                break;
            default:
                System.out.println("Unsupported message.");
        }
    }


    public static void main( String[] args ) throws URISyntaxException {
        WebsocketClient c = new WebsocketClient( new URI( "ws://localhost:8000/api" ), new BenchmarkHelper(), ".results/test");
        c.connect();
        c.close();
    }

}