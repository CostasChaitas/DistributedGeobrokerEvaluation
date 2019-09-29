package com.chaitas.distributed.geobroker.Benchmark;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.CONNECTPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.PUBLISHPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.SUBSCRIBEPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Geofence;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Location;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Topic;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.UtilityKt;
import com.chaitas.distributed.geobroker.Utils.KryoSerializerPool;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;

class BenchmarkClient implements Runnable {

    private final String apiURL = "ws://localhost:8000/api";
    private final String testsDirectoryPath = "./validation/";
    public static CountDownLatch latch = new CountDownLatch(1);
    private final KryoSerializerPool kryo = new KryoSerializerPool();
    private final String clientName;
    private WebsocketClient websocketClient;
    private final String readFilePath;

    public BenchmarkClient(String clientName) {
        System.out.println(" clientName is : " + clientName);
        this.clientName = clientName;
        this.readFilePath = testsDirectoryPath + clientName + ".csv";
        this.createClientWebsocket();
        this.connectClientWebsocket();
    }

    private void createClientWebsocket() {
        try {
            websocketClient = new WebsocketClient(new URI(apiURL));
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
        try {
            Location location = Location.random();
            ExternalMessage subscribe = new ExternalMessage(clientName, ControlPacketType.SUBSCRIBE,
                    new SUBSCRIBEPayload(new Topic("testTopic"), Geofence.circle(location, 0.0)));
            byte[] subscribeMsg = kryo.write(subscribe);
            websocketClient.send(subscribeMsg);


            ExternalMessage publish = new ExternalMessage(clientName, ControlPacketType.PUBLISH,
                    new PUBLISHPayload(new Topic("testTopic"), Geofence.circle(location, 0.0), "test"));
            byte[] publishMsg = kryo.write(publish);
            websocketClient.send(publishMsg);
        } catch (Exception e) {
            throw new Error(e);
        } finally {
            // Close Websocket connection after 3 seconds
            UtilityKt.sleepNoLog(3000, 0);
            websocketClient.close();
        }

//            try {
//                String line;
//                //Read to skip the header
//                br.readLine();
//                while ((line = br.readLine()) != null) {
//                    String[] messageDetails = line.split(";");
//                    if (messageDetails.length > 0) {
//                        writers.handleTestEntry(messageDetails);
//                    }
//                }
//
//            } catch (ParseException e) {
//                e.printStackTrace();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
//                try {
//                    fr.close();
//                    br.close();
//                } catch (IOException ex) {
//                    System.out.println(ex);
//                }
//            }
    }
}
