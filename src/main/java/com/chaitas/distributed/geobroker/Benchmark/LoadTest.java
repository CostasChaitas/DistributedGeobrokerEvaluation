package com.chaitas.distributed.geobroker.Benchmark;


import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.CONNECTPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Location;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.UtilityKt;
import com.chaitas.distributed.geobroker.Utils.JSONable;
import com.chaitas.distributed.geobroker.Utils.KryoSerializerPool;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;

import java.io.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class LoadTest {

    private KryoSerializerPool kryoSerializerPool = new KryoSerializerPool();
    private String testsDirectoryPath = "./validation/";
    private String resultsDirectoryPath = "./results/";
    private String apiURL = "ws://localhost:8000/api";

    public static void main (String[] args) throws Exception {
        LoadTest loadTest = new LoadTest();
        loadTest.setUp();
        loadTest.loadTests();
        loadTest.tearDown();
    }

    public void setUp() {
        System.out.println("Running setUp");
        BenchmarkHelper.startBenchmarking(resultsDirectoryPath);
    }

    public void tearDown() {
        System.out.println("Running tearDown after 3 seconds.");
        BenchmarkHelper.stopBenchmarking();
        UtilityKt.sleepNoLog(3000, 0);
        System.exit(0);
    }

    public void loadTests() throws InterruptedException, IOException {
        System.out.println("RUNNING testOneLocations");

        List<Thread> clients = new ArrayList<>();
        int numberOfClients = 0;

        InputStream is = Runtime.getRuntime().exec(new String[] {"bash", "-c", "ulimit -a"}).getInputStream();
        int c;
        while ((c = is.read()) != -1) {
            System.out.write(c);
        }

        File directory = new File(testsDirectoryPath);
        for (File f : Objects.requireNonNull(directory.listFiles())) {
            if (f.getName().endsWith(".csv")) {
                String fileNameWithOutExt = f.getName().replaceFirst("[.][^.]+$", "");
                System.out.println("Filename is " + fileNameWithOutExt);
                Thread client = new Thread(new BenchmarkClient(fileNameWithOutExt));
                numberOfClients++;
                clients.add(client);
            }
        }

        System.out.println("Waiting clients....");
        UtilityKt.sleepNoLog(3000, 0);
        System.out.println("Starting clients....");

        for (Thread client : clients) {
            client.start();
        }

        // wait for clients
        for (Thread client : clients) {
            client.join();
        }
        // wait for receiving to stop
        System.out.println("FINISHED");
    }

    class BenchmarkClient implements Runnable {

        public String clientName;
        public WebSocket websocket;
        public String readFilePath;
        FileReader fr;
        BufferedReader br;
        Writers writers;

        public BenchmarkClient(String clientName) {
            System.out.println(" clientName is : " + clientName);
            this.clientName = clientName;
            this.readFilePath = testsDirectoryPath + clientName + ".csv";
            BenchmarkHelper.startBenchmarking(readFilePath);
            try {
                fr = new FileReader(this.readFilePath);
                br = new BufferedReader(fr);
                createClientWebsocket();
                writers = new Writers(clientName, websocket);
                connectClientWebsocket();
            } catch (Exception e) {
                throw new Error(e);
            }
        }

        private void createClientWebsocket() {
            try {
                AsyncHttpClient c = asyncHttpClient();
                websocket = c.prepareGet(apiURL)
                        .execute(
                                new WebSocketUpgradeHandler.Builder()
                                        .addWebSocketListener(new WebSocketListener() {
                                            @Override
                                            public void onOpen(WebSocket websocket) {
                                                System.out.println("Connection opened...");
                                            }

                                            @Override
                                            public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                                                System.out.println(payload);
                                                try {
                                                    System.out.print("Received a text message");
                                                    Optional<ExternalMessage> message0 = JSONable.fromJSON(payload, ExternalMessage.class);
                                                    ExternalMessage message = message0.get();
                                                    writers.handleReceivedMessage(message);
                                                } catch (Exception e) {
                                                    System.out.println(e);
                                                    System.out.println("Received an incompatible Text Message: +" + payload);
                                                    throw new Error(e);
                                                }

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

        private void connectClientWebsocket() {
            Location location = Location.random();
            ExternalMessage connect = new ExternalMessage(clientName, ControlPacketType.CONNECT, new CONNECTPayload(location));
            byte[] connectMsg = kryoSerializerPool.write(connect);
            websocket.sendBinaryFrame(connectMsg);
        }

        @Override
        public void run () {
            System.out.println("Started client " + clientName);
            try {
                String line;
                //Read to skip the header
                br.readLine();
                while ((line = br.readLine()) != null) {
                    String[] messageDetails = line.split(";");
                    if (messageDetails.length > 0) {
                        writers.handleTestEntry(messageDetails);
                    }
                }

            } catch (ParseException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
