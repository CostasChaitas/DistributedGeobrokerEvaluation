package com.chaitas.distributed.geobroker.Benchmark;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class BenchmarkHelper {

    public static AtomicBoolean benchmarking = new AtomicBoolean(false);
    private static final ExecutorService pool = Executors.newSingleThreadExecutor();

    public static void startBenchmarking(String directoryPath) {
        File f = new File(directoryPath);
        if (f.mkdirs() || f.exists()) {
            BenchmarkHelper.benchmarking.set(true);
            System.out.println("Activated benchmarking");
        } else {
            System.out.println("Could not activate benchmarking");
        }
    }

    public static String getCSVHeader() {
        return "timestamp(ms);action_type;latitude;longitude;topic;geofence;payload_size\n";
    }

    public void addEntry(String fileName, String data) {
        if (!benchmarking.get()) {
            return;
        }
        pool.submit(() -> {
            System.out.println("Flushing data : " + data);
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter(fileName + ".csv", true));
                writer.write(String.valueOf(data));
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }

}
