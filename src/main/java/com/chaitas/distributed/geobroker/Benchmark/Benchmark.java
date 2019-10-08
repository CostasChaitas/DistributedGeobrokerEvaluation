// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Benchmark;

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
        try {
            Thread thread = new Thread(new BenchmarkClients(testsDirectoryPath, apiURL));
            thread.start();
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
