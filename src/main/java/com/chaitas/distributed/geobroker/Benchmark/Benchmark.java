package com.chaitas.distributed.geobroker.Benchmark;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.UtilityKt;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Benchmark {

    private final String testsDirectoryPath = "./validation/";

    public static void main (String[] args) throws Exception {
        Benchmark loadTest = new Benchmark();
        loadTest.setUp();
        loadTest.loadTests();
        loadTest.tearDown();
    }

    public void setUp() {
        System.out.println("Running setUp");
        BenchmarkHelper.startBenchmarking();
    }

    public void tearDown() {
        System.out.println("Running tearDown");
        BenchmarkHelper.stopBenchmarking();
        System.exit(0);
    }

    public void loadTests() throws IOException {
        System.out.println("RUNNING testOneLocations");

        List<BenchmarkClient> clients = new ArrayList<>();
        int numberOfClients = new File(testsDirectoryPath).listFiles().length;

        InputStream is = Runtime.getRuntime().exec(new String[] {"bash", "-c", "ulimit -a"}).getInputStream();
        int c;
        while ((c = is.read()) != -1) {
            System.out.write(c);
        }

        File directory = new File(testsDirectoryPath);
        for (File f : Objects.requireNonNull(directory.listFiles())) {
            if (f.getName().endsWith(".csv")) {
                String fileNameWithOutExt = f.getName().replaceFirst("[.][^.]+$", "");
                clients.add(new BenchmarkClient(fileNameWithOutExt));
                numberOfClients++;
                System.out.println("New thread: " + fileNameWithOutExt);
            }
        }

        System.out.println("There are " + clients.size() + " clients");
        UtilityKt.sleepNoLog(3000, 0);
        System.out.println("Starting clients....");

        ExecutorService pool = Executors.newFixedThreadPool(numberOfClients);

        CompletableFuture<?>[] futures = clients.stream()
                .map(task -> CompletableFuture.runAsync(task, pool))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();

        System.out.println("Terminating threads in 3 seconds...");
        UtilityKt.sleepNoLog(3000, 0);
        pool.shutdown();
        // wait for receiving to stop
        System.out.println("All Threads have been terminated");
    }

}
