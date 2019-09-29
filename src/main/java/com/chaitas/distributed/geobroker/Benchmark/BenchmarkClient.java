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

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.concurrent.CountDownLatch;

class BenchmarkClient implements Runnable {

    private final String apiURL = "ws://localhost:8000/api";
    private final String readFilePath;
    public static CountDownLatch latch = new CountDownLatch(1);
    private final KryoSerializerPool kryo = new KryoSerializerPool();
    private final String clientName;
    private WebsocketClient websocketClient;

    public BenchmarkClient(String clientName) {
        System.out.println(" clientName is : " + clientName);
        this.clientName = clientName;
        this.readFilePath = "./validation/" + clientName + ".csv";
        try {
            websocketClient = new WebsocketClient(new URI("ws://localhost:8000/api"));
            this.createClientWebsocket();
            this.connectClientWebsocket();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private void createClientWebsocket() {
        try {
            websocketClient.connectBlocking();
        } catch (InterruptedException e) {
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
                    byte[] obj = parseTestEntry(messageDetails);
                    long time = System.nanoTime();
                    ExternalMessage message = websocketClient.sendAndReceive(obj, 1000);
                    BenchmarkHelper.addEntry(message.getControlPacketType().toString(), System.nanoTime() - time);
                }
            }
        } catch (Exception e) {
            throw new Error(e);
        } finally {
            websocketClient.close();
        }
    }

    public byte[] parseTestEntry (String[]messageDetails) throws ParseException {
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
                return pingMsg;
            case "subscribe":
                Topic subTopic = new Topic(messageDetails[4]);
                Geofence subGeofence = new Geofence(messageDetails[5]);
                ExternalMessage subscribe = new ExternalMessage(
                        clientName,
                        ControlPacketType.SUBSCRIBE,
                        new SUBSCRIBEPayload(subTopic, subGeofence)
                );
                byte[] subscribeMsg = kryo.write(subscribe);
                return subscribeMsg;
            case "publish":
                Topic pubTopic = new Topic(messageDetails[4]);
                Geofence pubGeofence = new Geofence(messageDetails[5]);
                ExternalMessage publish = new ExternalMessage(
                        clientName,
                        ControlPacketType.PUBLISH,
                        new PUBLISHPayload(pubTopic, pubGeofence, "Publishing some cool stuff.")
                );
                byte[] publishMsg = kryo.write(publish);
                return publishMsg;
            default:
                System.out.println("Unsupported message.");
                throw new Error("Cannot parse Test Entry");

        }
    }
}
