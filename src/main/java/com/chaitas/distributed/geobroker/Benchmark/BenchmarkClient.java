package com.chaitas.distributed.geobroker.Benchmark;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.PINGREQPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.PUBLISHPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.SUBSCRIBEPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Geofence;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Location;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Topic;

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
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private WebsocketClient websocketClient;
    private Long initialTime;
    private Long maxMessageTime = 0L;

    public BenchmarkClient(String clientName, String testsDirectoryPath, String apiURL) {
        this.clientName = clientName;
        this.testsDirectoryPath = testsDirectoryPath;
        this.apiURL = apiURL;
        try {
            websocketClient = new WebsocketClient(new URI(this.apiURL), clientName);
            websocketClient.connectBlocking();
            websocketClient.connectClientWebsocket();
        } catch (URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run () {
        initialTime = System.currentTimeMillis();
        websocketClient.setTime(initialTime);
        System.out.println("Running client " + clientName);

        String readFilePath = this.testsDirectoryPath + clientName + ".csv";
        try (BufferedReader br = new BufferedReader(new FileReader(readFilePath))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] messageDetails = line.split(";");
                if (messageDetails.length > 0) {
                    Long timeToSend = Long.parseLong(messageDetails[0]);
                    Long delay = timeToSend - (System.currentTimeMillis() - initialTime);
                    if(timeToSend > maxMessageTime){
                        maxMessageTime = timeToSend;
                    }

                    executor.schedule(() -> {
                        try {
                            ExternalMessage message = parseEntry(messageDetails, clientName);
                            websocketClient.sendMessage(message);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                    }, delay, TimeUnit.MILLISECONDS);

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
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
