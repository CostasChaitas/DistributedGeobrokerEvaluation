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

class BenchmarkClient implements Runnable {

    private final String clientName;
    private final String testsDirectoryPath;
    private final String apiURL;
    private final KryoSerializerPool kryo = new KryoSerializerPool();
    private WebsocketClient websocketClient;
    private Long time;

    public BenchmarkClient(String clientName, String testsDirectoryPath, String apiURL) {
        this.clientName = clientName;
        this.testsDirectoryPath = testsDirectoryPath;
        this.apiURL = apiURL;
        try {
            websocketClient = new WebsocketClient(new URI(this.apiURL), clientName);
            this.createClientWebsocket();
            if(websocketClient.isOpen() == true) {
                this.connectClientWebsocket();
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
        Location location = Location.random();
        ExternalMessage connect = new ExternalMessage(websocketClient.getClientName(), ControlPacketType.CONNECT, new CONNECTPayload(location));
        byte[] connectMsg = kryo.write(connect);
        websocketClient.send(connectMsg);
    }

    @Override
    public void run () {
        time = System.currentTimeMillis();
        websocketClient.setTime(time);
        System.out.println("Running client " + clientName);

        String readFilePath = this.testsDirectoryPath + clientName + ".csv";
        try (BufferedReader br = new BufferedReader(new FileReader(readFilePath))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] messageDetails = line.split(";");
                if (messageDetails.length > 0) {
                    Long timeToSend = Long.parseLong(messageDetails[0]);
                    Long delay = timeToSend - (System.currentTimeMillis() - time);
                    System.out.println(clientName + " waiting to send message... : " + delay);
                    while(delay >= 0) {
                        Thread.sleep(1);
                        delay = timeToSend - (System.currentTimeMillis() - time);
                    }
                    ExternalMessage message = parseAndSendEntry(messageDetails, clientName, websocketClient);
                    BenchmarkHelper.addEntry(message.getControlPacketType().toString(), clientName, System.currentTimeMillis() - time);
                }
            }
        } catch (Exception e) {
            System.out.println(e);
            throw new Error(e);
        }

        try {
            Thread.sleep( 5000);
            System.out.println("Closing Websocket " + clientName);
            websocketClient.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public ExternalMessage parseAndSendEntry (String[] messageDetails, String clientName, WebsocketClient websocket) throws ParseException {
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
                websocket.send(pingMsg);
                return ping;
            case "subscribe":
                Topic subTopic = new Topic(messageDetails[4]);
                Geofence subGeofence = new Geofence(messageDetails[5]);
                ExternalMessage subscribe = new ExternalMessage(
                        clientName,
                        ControlPacketType.SUBSCRIBE,
                        new SUBSCRIBEPayload(subTopic, subGeofence)
                );
                byte[] subscribeMsg = kryo.write(subscribe);
                websocket.send(subscribeMsg);
                return subscribe;
            case "publish":
                Topic pubTopic = new Topic(messageDetails[4]);
                Geofence pubGeofence = new Geofence(messageDetails[5]);
                ExternalMessage publish = new ExternalMessage(
                        clientName,
                        ControlPacketType.PUBLISH,
                        new PUBLISHPayload(pubTopic, pubGeofence, clientName + "Publishing some cool stuff.")
                );
                byte[] publishMsg = kryo.write(publish);
                websocket.send(publishMsg);
                return publish;
            default:
                System.out.println("Unsupported message.");
                throw new Error("Cannot parse Test Entry");
        }
    }


}
