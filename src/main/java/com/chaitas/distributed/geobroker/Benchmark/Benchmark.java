// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Benchmark;

import sun.jvm.hotspot.runtime.Threads;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

public class Benchmark {

    private final String testsDirectoryPath = "./validation/";
    private final String apiURL = "ws://localhost:8000/api";

    public static void main (String[] args) {
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

    public void loadTests() {
        System.out.println("Running Benchmark");
        System.out.println("Available processors: " + Runtime.getRuntime().availableProcessors());

        List<Thread> runnables = new ArrayList<>();
        Integer numOfClients = 0;

        File directory = new File(this.testsDirectoryPath);
        for (File f : Objects.requireNonNull(directory.listFiles())) {
            if (f.getName().endsWith(".csv")) {
                String clientName = f.getName().replaceFirst("[.][^.]+$", "");
                Thread thread = new Thread(new BenchmarkClient(clientName, testsDirectoryPath, apiURL));
                runnables.add(thread);
                numOfClients++;
            }
        }

        List<Callable<Void>> callables = new ArrayList<>();
        for (Runnable r : runnables) {
            callables.add(toCallable(r));
        }

        ExecutorService pool = Executors.newFixedThreadPool(numOfClients);
        try {
            pool.invokeAll(callables);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    // Convert Runnable to Callable
    private Callable<Void> toCallable(final Runnable runnable) {
        return () -> {
            runnable.run();
            return null;
        };
    }

}
