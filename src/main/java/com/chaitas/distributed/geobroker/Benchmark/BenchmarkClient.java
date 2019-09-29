package com.chaitas.distributed.geobroker.Benchmark;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.CONNECTPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.PINGREQPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.PUBLISHPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.SUBSCRIBEPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Geofence;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Location;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Topic;
import com.chaitas.distributed.geobroker.Utils.KryoSerializerPool;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.concurrent.CountDownLatch;

class BenchmarkClient implements Runnable {

    private final String apiURL = "ws://localhost:8000/api";
    private final String readFilePath;
    private final String writeFilePath;
    public static CountDownLatch latch = new CountDownLatch(1);
    private final KryoSerializerPool kryo = new KryoSerializerPool();
    private final String clientName;
    private WebsocketClient websocketClient;
    private BenchmarkHelper benchmarkHelper = new BenchmarkHelper();

    public BenchmarkClient(String clientName) {
        System.out.println(" clientName is : " + clientName);
        this.clientName = clientName;
        this.readFilePath = "./validation/" + clientName + ".csv";
        this.writeFilePath = "./results/" + clientName + ".csv";
        benchmarkHelper.addEntry(writeFilePath, BenchmarkHelper.getCSVHeader());
        this.createClientWebsocket();
        this.connectClientWebsocket();
    }

    private void createClientWebsocket() {
        try {
            websocketClient = new WebsocketClient(new URI(apiURL), benchmarkHelper, writeFilePath);
            websocketClient.connectBlocking();
        } catch (URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void connectClientWebsocket() {
        Location location = Location.random();
        ExternalMessage connect = new ExternalMessage(clientName, ControlPacketType.CONNECT, new CONNECTPayload(location));
        byte[] connectMsg = kryo.write(connect);
        websocketClient.send(connectMsg);
    }


    @Override
    public void run () {
        System.out.println("Started client " + clientName);
        try (BufferedReader br = new BufferedReader(new FileReader(readFilePath))){

            String line;
            //Read to skip the header
            br.readLine();
            while ((line = br.readLine()) != null) {
                latch = new CountDownLatch(1);
                String[] messageDetails = line.split(";");
                if (messageDetails.length > 0) {
                    createStringFromTestEntry(messageDetails);
                }
            }
        } catch (Exception e) {
            throw new Error(e);
        } finally {
            websocketClient.close();
        }

    }


    public void createStringFromTestEntry (String[]messageDetails) throws ParseException {
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
                byte[] pingMsg = kryo.write(ping);
                long timePing = System.nanoTime();
                websocketClient.send(pingMsg);
                String pingText = String.format("%2d;PINGREQ;%f;%f;;;;\n", timePing, lat, lon);
                benchmarkHelper.addEntry(writeFilePath, pingText);
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
                byte[] subscribeMsg = kryo.write(subscribe);
                long timeSub = System.nanoTime();
                websocketClient.send(subscribeMsg);
                String subText = String.format("%2d;SUBSCRIBE;%f;%f;%s;%s;\n", timeSub, lat, lon, subTopic.getTopic(), subGeofence.getWKT());
                benchmarkHelper.addEntry(writeFilePath, subText);
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
                byte[] publishMsg = kryo.write(publish);
                long timePub = System.nanoTime();
                websocketClient.send(publishMsg);
                String pubText = String.format("%2d;PUBLISHSEND;%f;%f;%s;%s;\n", timePub, lat, lon, pubTopic.getTopic(), pubGeofence.getWKT());
                System.out.println(clientName + "Sending PUBLISHSEND");
                benchmarkHelper.addEntry(writeFilePath, pubText);
                break;
            default:
                System.out.println("Unsupported message.");
                break;

        }
    }
}
