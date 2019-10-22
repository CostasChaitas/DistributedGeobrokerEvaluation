// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Benchmark;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ExternalMessage;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.PINGREQPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.PUBLISHPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Payloads.SUBSCRIBEPayload;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Geofence;
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Topic;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class BenchmarkHelper {

   private static final ExecutorService pool = Executors.newSingleThreadExecutor();

    public static final int CAPACITY = 100000;
    public static final int FLUSH_COUNT = CAPACITY / 2;
    public static final String directoryPath = "./benchmarking_results/";
    public static AtomicBoolean benchmarking = new AtomicBoolean(false);

    private static final BlockingQueue<BenchmarkEntry> benchmarkEntryStorage = new ArrayBlockingQueue<>(CAPACITY);
    private static final AtomicInteger putElementCount = new AtomicInteger(0);

    public static void startBenchmarking() {
        File f = new File(directoryPath);
        if (f.mkdirs() || f.exists()) {
            BenchmarkHelper.benchmarking.set(true);
            System.out.println("Activated benchmarking");
        } else {
            System.out.println("Could not activate benchmarking");
        }
    }

    public static void addEntry(String name, String clientName, long time) {
        if (!benchmarking.get()) {
            return;
        }

        BenchmarkEntry entry = new BenchmarkEntry(name, clientName, System.currentTimeMillis(), time);
        int timeToFlush = putElementCount.incrementAndGet() % FLUSH_COUNT;
        benchmarkEntryStorage.offer(entry);
        if (timeToFlush == 0) {
            // FLUSH_COUNT elements have been added since last flush, so time to flush
            pool.submit(() -> {
                System.out.println("Flushing benchmarking values");
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(directoryPath + System.nanoTime() + ".csv"))) {
                    writer.write(BenchmarkEntry.getCSVHeader());
                    for (int i = 0; i < FLUSH_COUNT; i++) {
                        writer.write(String.valueOf(benchmarkEntryStorage.poll()));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public static void stopBenchmarking() {
        BenchmarkHelper.benchmarking.set(false);
        pool.submit(() -> {
            System.out.println("Flushing benchmarking values");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(directoryPath + System.nanoTime() + ".csv"))) {
                writer.write(BenchmarkEntry.getCSVHeader());
                Integer benchmarkEntryStorageLenght = benchmarkEntryStorage.size();
                System.out.println("There are " + benchmarkEntryStorageLenght + " entries to be stored");
                for (int i = 0; i < benchmarkEntryStorageLenght; i++) {
                    writer.write(String.valueOf(benchmarkEntryStorage.poll()));
                    writer.flush();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        pool.shutdown();
        try {
            pool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.out.println("Did not terminate properly" + e);
        }
        System.out.println("Deactivated benchmarking");
    }

    public static class BenchmarkEntry {
        public String name;
        public String clientName;
        public long timestamp;
        public long time;

        public BenchmarkEntry(String name, String clientName, long timestamp, long time) {
            this.name = name;
            this.clientName = clientName;
            this.timestamp = timestamp;
            this.time = time;
        }

        public static BenchmarkEntry fromString(String s) {
            String[] entries = s.split(";");
            if (entries.length == 4) {
                return new BenchmarkEntry(entries[0], entries[1], Long.valueOf(entries[2]), Long.valueOf(entries[3]));
            } else {
                return new BenchmarkEntry("null", "null",0, 0);
            }
        }

        public static String getCSVHeader() {
            return "name;clientName;timestamp;time(ms)\n";
        }

        @Override
        public String toString() {
            return String.format("%s;%s;%d;%d\n", name, clientName, timestamp, time);
        }
    }

    /*****************************************************************
     * Post-Processing
     ****************************************************************/

    // contains the post-processing file names for raw data
    private static ArrayList<String> filePaths = new ArrayList<>();
    private static ArrayList<String> filePathsResults = new ArrayList<>();
    private static ArrayList<String> filePathsMessageTypes = new ArrayList<>();

    public enum SortType {
        clientName,
        name
    }

    public BenchmarkHelper() {

    }

    public void sortIntoFiles(ArrayList<String> sortedList, SortType sortType) throws IOException {
        System.out.println("Starting sort process");
        File directory = new File(BenchmarkHelper.directoryPath);
        HashMap<String, BufferedWriter> writers = new HashMap<>();

        // read in each file
        for (File f : Objects.requireNonNull(directory.listFiles())) {
            System.out.println("Starting with file {}" +  f.getName());
            Stream<String> stream = Files.lines(Paths.get(f.getPath()));
            stream.forEach(line -> {
                try {
                    // we do not want the header
                    if (!line.startsWith(BenchmarkEntry.getCSVHeader().substring(0, 5))) {

                        BenchmarkEntry entry = BenchmarkEntry.fromString(line);
                        String type = sortType == SortType.clientName ? entry.clientName : entry.name;

                        // get correct writer or create
                        BufferedWriter writer = writers.get(type);
                        if (writer == null) {
                            System.out.println("Creating writer for {}" + type);
                            String filePath = BenchmarkHelper.directoryPath + type + ".csv";
                            sortedList.add(filePath);
                            writer = new BufferedWriter(new FileWriter(new File(filePath)));
                            writers.put(type, writer);
                            writer.write(BenchmarkEntry.getCSVHeader());
                        }

                        // write to correct writer
                        writer.write(entry.toString());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            System.out.println("Finished file {}" + f.getName());
        }
        // close all writers
        for (BufferedWriter writer : writers.values()) {
            writer.close();
        }
    }

    public ControlPacketType mapMessageTypes(ControlPacketType controlPacketType) {
        switch (controlPacketType) {
            case CONNECT:
                return controlPacketType.CONNACK;
            case PINGREQ:
                return controlPacketType.PINGRESP;
            case SUBSCRIBE:
                return controlPacketType.SUBACK;
            case UNSUBSCRIBE:
                return controlPacketType.UNSUBACK;
            case PUBLISH:
                return controlPacketType.PUBACK;
            default:
                return controlPacketType.INCOMPATIBLEPayload;
        }
    }

    public void calculateResponses(String filePath) throws IOException {

        System.out.println(filePath);
        String resultfilePath = filePath.replaceFirst(".csv", "") + "-results.csv";
        BenchmarkEntry entryReq = null;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath));
             BufferedWriter wr = new BufferedWriter(new FileWriter(new File(resultfilePath))) ) {

            wr.write(BenchmarkEntry.getCSVHeader());
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                BenchmarkEntry entry = BenchmarkEntry.fromString(line);
                if (entry.name == null) {
                    throw new RuntimeException("Nulls not supported");
                }
                if(entry.name.equals("PUBLISH_RECEIVED")){
                    continue;
                }

                if(entryReq != null && ControlPacketType.valueOf(entry.name) == mapMessageTypes(ControlPacketType.valueOf(entryReq.name))) {
                    entry.time = entry.time - entryReq.time;
                    wr.write(entry.toString());
                    filePathsResults.add(resultfilePath);
                    entryReq = null;
                } else {
                    entryReq = entry;
                }

            }
        }

    }

    public void writeStatisticsForFile(String filePath) throws IOException {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        // read in data
        Stream<String> stream = Files.lines(Paths.get(filePath));
        stream.forEach(line -> {
            // we do not want the header
            if (!line.startsWith(BenchmarkEntry.getCSVHeader().substring(0, 5))) {

                BenchmarkEntry entry = BenchmarkEntry.fromString(line);
                // no null entries should exist, let's check anyhow
                if (entry.name == null) {
                    throw new RuntimeException("Nulls not supported");
                }
                stats.addValue(entry.time);

            }
        });

        double n = stats.getN();
        double mean = stats.getMean();
        double std = stats.getStandardDeviation();
        double median = stats.getPercentile(50);

        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath.replaceFirst(".csv", "") + "-stats.csv"));
        writer.write("n;" + n + "\n");
        writer.write("mean;" + mean + "\n");
        writer.write("standard deviation;" + std + "\n");
        writer.write("median;" + median + "\n");
        writer.close();
    }

    public static void main (String[] args) throws IOException {
        BenchmarkHelper helper = new BenchmarkHelper();

        helper.sortIntoFiles(filePaths, SortType.clientName);

        for (String path : helper.filePaths) {
            helper.calculateResponses(path);
        }

        helper.sortIntoFiles(filePathsMessageTypes, SortType.name);

        for (String path : helper.filePathsMessageTypes) {
            helper.writeStatisticsForFile(path);
        }
    }

}
