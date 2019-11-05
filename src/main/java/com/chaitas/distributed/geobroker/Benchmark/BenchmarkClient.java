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
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class BenchmarkClient implements Runnable {

    private final String clientName;
    private final String testsDirectoryPath;
    private final String apiURL;
    private final KryoSerializerPool kryo = new KryoSerializerPool();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private WebsocketClient websocketClient;
    private Long time;
    private Long maxMessageTime = 0L;

    public BenchmarkClient(String clientName, String testsDirectoryPath, String apiURL) {
        this.clientName = clientName;
        this.testsDirectoryPath = testsDirectoryPath;
        this.apiURL = apiURL;
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
        ExternalMessage connect = new ExternalMessage(UUID.randomUUID().toString(), websocketClient.getClientName(), ControlPacketType.CONNECT, new CONNECTPayload(location));
        byte[] connectMsg = kryo.write(connect);
        websocketClient.send(connectMsg);
    }

    @Override
    public void run () {
        try {
            websocketClient = new WebsocketClient(new URI(this.apiURL), clientName);
            this.createClientWebsocket();
            this.connectClientWebsocket();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

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
                    if(timeToSend > maxMessageTime){
                        maxMessageTime = timeToSend;
                    }

                    executor.schedule(() -> {
                        try {
                            ExternalMessage message = parseEntry(messageDetails, clientName);
                            byte[] arr = kryo.write(message);
                            Long timeNowMillis = System.currentTimeMillis() - time;
                            BenchmarkHelper.addEntry(message.getControlPacketType().toString(), message.getId(), clientName, timeNowMillis);
                            websocketClient.send(arr);
                            System.out.println(clientName + " sending message : " + message.getControlPacketType().toString());
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                    }, delay, TimeUnit.MILLISECONDS);

                }
            }
        } catch (Exception e) {
            System.out.println(e);
            throw new Error(e);
        }

        try {
            executor.shutdown();
            executor.awaitTermination(maxMessageTime + 5000, TimeUnit.MILLISECONDS);
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Closing Websocket " + clientName);
            websocketClient.close();
        }
    }


    public ExternalMessage parseEntry (String[] messageDetails, String clientName) throws ParseException {
        String controlPacket = messageDetails[3];
        switch (controlPacket) {
            case "ping":
                double lat = Double.parseDouble(messageDetails[1]);
                double lon = Double.parseDouble(messageDetails[2]);
                ExternalMessage ping = new ExternalMessage(
                        UUID.randomUUID().toString().replace("-", ""),
                        clientName,
                        ControlPacketType.PINGREQ,
                        new PINGREQPayload(new Location(lat, lon))
                );
                return ping;
            case "subscribe":
                Topic subTopic = new Topic(messageDetails[4]);
                Geofence subGeofence = new Geofence(messageDetails[5]);
                ExternalMessage subscribe = new ExternalMessage(
                        UUID.randomUUID().toString().replace("-", ""),
                        clientName,
                        ControlPacketType.SUBSCRIBE,
                        new SUBSCRIBEPayload(subTopic, subGeofence)
                );
                return subscribe;
            case "publish":
                Topic pubTopic = new Topic(messageDetails[4]);
                Geofence pubGeofence = new Geofence(messageDetails[5]);
                ExternalMessage publish = new ExternalMessage(
                        UUID.randomUUID().toString().replace("-", ""),
                        clientName,
                        ControlPacketType.PUBLISH,
                        new PUBLISHPayload(pubTopic, pubGeofence, clientName + ": Publishing some cool stuff.")
                );
                return publish;
            default:
                System.out.println("Unsupported message.");
                throw new Error("Cannot parse Test Entry");
        }
    }


}
