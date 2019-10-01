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
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class BenchmarkClients implements Runnable {

    private final String testsDirectoryPath;
    private final String apiURL;
    private final KryoSerializerPool kryo = new KryoSerializerPool();
    private WebsocketClient websocketClient;
    private List<WebsocketClient> websocketList = new ArrayList<>();

    public BenchmarkClients(String testsDirectoryPath, String apiURL) {
        this.testsDirectoryPath = testsDirectoryPath;
        this.apiURL = apiURL;
        try {
            File directory = new File(this.testsDirectoryPath);
            for (File f : Objects.requireNonNull(directory.listFiles())) {
                if (f.getName().endsWith(".csv")) {
                    String fileNameWithOutExt = f.getName().replaceFirst("[.][^.]+$", "");
                    websocketClient = new WebsocketClient(new URI(this.apiURL), fileNameWithOutExt);
                    websocketList.add(websocketClient);
                    this.createClientWebsocket();
                    this.connectClientWebsocket();
                }
            }
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
        try {
            Location location = Location.random();
            ExternalMessage connect = new ExternalMessage(websocketClient.getClientName(), ControlPacketType.CONNECT, new CONNECTPayload(location));
            byte[] connectMsg = kryo.write(connect);
            websocketClient.sendAndReceive(connectMsg, 2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run () {
        websocketList.parallelStream().forEach(websocket -> {
            String clientName = new String(websocket.getClientName());
            String readFilePath = this.testsDirectoryPath + clientName + ".csv";
            System.out.println("Started client " + clientName);
            try (BufferedReader br = new BufferedReader(new FileReader(readFilePath))){
                String line;
                br.readLine();
                while ((line = br.readLine()) != null) {
                    String[] messageDetails = line.split(";");
                    if (messageDetails.length > 0) {
                        byte[] obj = parseTestEntry(messageDetails, clientName);
                        long time = System.nanoTime();
                        ExternalMessage message = websocket.sendAndReceive(obj, 5000);
                        BenchmarkHelper.addEntry(message.getControlPacketType().toString(), clientName,System.nanoTime() - time);
                    }
                }
            } catch (Exception e) {
                System.out.println(e);
                throw new Error(e);
            } finally {
                websocket.close();
            }
        });
    }


    public byte[] parseTestEntry (String[]messageDetails, String clientName) throws ParseException {
        String controlPacket = messageDetails[3];
        switch (controlPacket) {
            case "ping":
                double lat = Double.parseDouble(messageDetails[1]);
                double lon = Double.parseDouble(messageDetails[2]);
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
