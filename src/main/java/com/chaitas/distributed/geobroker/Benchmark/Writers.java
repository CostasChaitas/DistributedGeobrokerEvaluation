package com.chaitas.distributed.geobroker.Benchmark;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.PINGREQPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.PUBLISHPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.SUBSCRIBEPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Geofence;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Location;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Topic;
import com.chaitas.distributed.geobroker.Utils.KryoSerializerPool;
import org.asynchttpclient.ws.WebSocket;

import java.io.*;
import java.text.ParseException;


public class Writers {

    private KryoSerializerPool kryoSerializerPool = new KryoSerializerPool();
    private String resultsDirectoryPath = "./results/";
    private String clientName;
    private WebSocket websocket;
    private String writeFilePath;
    private FileWriter fw;
    private BufferedWriter wr;

    public Writers(String clientName, WebSocket websocket) {
        this.clientName = clientName;
        this.websocket = websocket;
        this.writeFilePath = resultsDirectoryPath + clientName + ".csv";
    }

    public void handleReceivedMessage(ExternalMessage message) throws IOException {
        try (Writer wr = new FileWriter(writeFilePath)){
            ControlPacketType controlPacketType = message.getControlPacketType();
            long time = System.nanoTime();
            switch (controlPacketType) {
                case CONNACK:
                    String connackText = String.format("%2d;%s;;;;;\n", time, "CONNACK");
                    wr.write(connackText);
                    break;
                case PINGRESP:
                    String pingrespText = String.format("%2d;%s;;;;;\n", time, "PINGRESP");
                    wr.write(pingrespText);
                    break;
                case SUBACK:
                    String subackText = String.format("%2d;%s;;;;;\n", time, "SUBACK");
                    wr.write(subackText);
                    break;
                case PUBACK:
                    String pubackText = String.format("%2d;%s;;;;;\n", time, "PUBACK");
                    wr.write(pubackText);
                    break;
                case PUBLISH:
                    String publishText = String.format("%2d;%s;;;;;\n", time, "PUBLISH");
                    wr.write(publishText);
                    break;
                case INCOMPATIBLEPayload:
                    String incompatibleText = String.format("%2d;%s;;;;;\n", time, "INCOMPATIBLEPayload");
                    wr.write(incompatibleText);
                    break;
                default:
                    System.out.println("Unsupported message.");
            }
        } finally {
            try {
                wr.close();
            } catch (IOException ex) {
                System.out.println(ex);

            }
        }
    }

    public void handleTestEntry (String[]messageDetails) throws IOException, ParseException {
        try (Writer wr = new FileWriter(writeFilePath)) {
            String controlPacket = messageDetails[3];
            double lat = Double.parseDouble(messageDetails[1]);
            double lon = Double.parseDouble(messageDetails[2]);
            switch (controlPacket) {
                case "ping":
                    ExternalMessage ping = new ExternalMessage(
                            clientName,
                            ControlPacketType.PINGREQ,
                            new PINGREQPayload(new Location(lat, lon))
                    );
                    byte[] pingMsg = kryoSerializerPool.write(ping);
                    long timePing = System.nanoTime();
                    websocket.sendBinaryFrame(pingMsg);
                    String pingText = String.format("%2d;PINGREQ;%f;%f;;;;\n", timePing, lat, lon);
                    wr.write(pingText);
                    System.out.println(clientName + "Sending PINGREQ");
                    break;
                case "subscribe":
                    Topic subTopic = new Topic(messageDetails[4]);
                    Geofence subGeofence = new Geofence(messageDetails[5]);
                    ExternalMessage subscribe = new ExternalMessage(
                            clientName,
                            ControlPacketType.SUBSCRIBE,
                            new SUBSCRIBEPayload(subTopic, subGeofence)
                    );
                    byte[] subscribeMsg = kryoSerializerPool.write(subscribe);
                    long timeSub = System.nanoTime();
                    websocket.sendBinaryFrame(subscribeMsg);
                    String subText = String.format("%2d;SUBSCRIBE;%f;%f;%s;%s;\n", timeSub, lat, lon, subTopic.getTopic(), subGeofence.getWKT());
                    wr.write(subText);
                    System.out.println(clientName + "Sending SUBSCRIBE");
                    break;
                case "publish":
                    Topic pubTopic = new Topic(messageDetails[4]);
                    Geofence pubGeofence = new Geofence(messageDetails[5]);
                    ExternalMessage publish = new ExternalMessage(
                            clientName,
                            ControlPacketType.PUBLISH,
                            new PUBLISHPayload(pubTopic, pubGeofence, "Publishing some cool stuff.")
                    );
                    byte[] publishMsg = kryoSerializerPool.write(publish);
                    long timePub = System.nanoTime();
                    websocket.sendBinaryFrame(publishMsg);
                    String pubText = String.format("%2d;PUBLISHSEND;%f;%f;%s;%s;\n", timePub, lat, lon, pubTopic.getTopic(), pubGeofence.getWKT());
                    System.out.println(clientName + "Sending PUBLISHSEND");
                    wr.write(pubText);
                    break;
                default:
                    System.out.println("Unsupported message.");
                    break;

            }
        }finally {
            try {
                wr.close();
            } catch (IOException ex) {
                System.out.println(ex);
            }
        }

    }
}
