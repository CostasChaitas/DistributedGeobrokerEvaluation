// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Benchmark;

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.ControlPacketType;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
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

    public static void addEntry(String name, String messageId, String clientName, long time) {
        if (!benchmarking.get()) {
            return;
        }

        BenchmarkEntry entry = new BenchmarkEntry(name, messageId, clientName, System.currentTimeMillis(), time);
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
        public String messageId;
        public String clientName;
        public long timestamp;
        public long time;

        public BenchmarkEntry(String name, String messageId, String clientName, long timestamp, long time) {
            this.name = name;
            this.messageId = messageId;
            this.clientName = clientName;
            this.timestamp = timestamp;
            this.time = time;
        }

        public static BenchmarkEntry fromString(String s) {
            String[] entries = s.split(";");
            if (entries.length == 5) {
                return new BenchmarkEntry(entries[0], entries[1], entries[2], Long.valueOf(entries[3]), Long.valueOf(entries[4]));
            } else {
                return new BenchmarkEntry("null", "null","null",0, 0);
            }
        }

        public static String getCSVHeader() {
            return "name;messageId;clientName;timestamp;time(ms)\n";
        }

        @Override
        public String toString() {
            return String.format("%s;%s;%s;%d;%d\n", name, messageId, clientName, timestamp, time);
        }
    }

    /*****************************************************************
     * Post-Processing
     ****************************************************************/

    // contains the post-processing file names for raw data
    private static ArrayList<String> filePathsClients = new ArrayList<>();
    private static ArrayList<String> filePathsSortedClients = new ArrayList<>();
    private static ArrayList<String> filePathsResults = new ArrayList<>();
    private static ArrayList<String> filePathsMessageTypes = new ArrayList<>();

    public enum SortType {
        clientName,
        name
    }

    public BenchmarkHelper() {

    }

    public void sortIntoFiles(String directoryName, String resultsPath, ArrayList<String> sortedList, SortType sortType) throws IOException {
        System.out.println("Starting sort process");
        File directory = new File(BenchmarkHelper.directoryPath + directoryName);
        HashMap<String, BufferedWriter> writers = new HashMap<>();

        // read in each file
        for (File f : Objects.requireNonNull(directory.listFiles())) {
            if(new File(f.getPath()).isFile()) {
                Stream<String> stream = Files.lines(Paths.get(f.getPath()), Charset.forName("ISO-8859-1"));
                stream.forEach(line -> {
                    try {
                        // we do not want the header
                        if (!line.startsWith(BenchmarkEntry.getCSVHeader().substring(0, 5))) {

                            BenchmarkEntry entry = BenchmarkEntry.fromString(line);
                            String type = sortType == SortType.clientName ? entry.clientName : entry.name;

                            // get correct writer or create
                            BufferedWriter writer = writers.get(type);
                            if (writer == null) {
                                File file = new File(BenchmarkHelper.directoryPath + resultsPath);
                                if (file.mkdirs() || file.exists()) {
                                    String filePath = BenchmarkHelper.directoryPath + resultsPath + type + ".csv";
                                    if(filePath.contains("null.csv")) return;
                                    writer = new BufferedWriter(new FileWriter(filePath));
                                    sortedList.add(filePath);
                                    writers.put(type, writer);
                                    writer.write(BenchmarkEntry.getCSVHeader());
                                } else {
                                    System.out.println("Could not find directory path : " + BenchmarkHelper.directoryPath + resultsPath);
                                }
                            }

                            // write to correct writer
                            writer.write(entry.toString());
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }

        }
        // close all writers
        for (BufferedWriter writer : writers.values()) {
            writer.close();
        }
    }


    public void sortByMessageTime(String directoryName, String resultsPath, ArrayList<String> sortedList) throws IOException {
        System.out.println("Starting sort process by message time");
        File directory = new File(BenchmarkHelper.directoryPath + directoryName);
        File resultsDirectory = new File(BenchmarkHelper.directoryPath + resultsPath);

        HashMap<String, BufferedWriter> writers = new HashMap<>();

        if (resultsDirectory.mkdirs() || resultsDirectory.exists()) {
            // read in each file
            for (File f : Objects.requireNonNull(directory.listFiles())) {
                if(new File(f.getPath()).isFile()) {

                    String filePath =  f.getPath().replaceFirst("clients", resultsPath).replaceFirst(".csv", "") + "-sorted.csv";

                    try (BufferedReader br = new BufferedReader(new FileReader(f));
                         BufferedWriter wr = new BufferedWriter(new FileWriter(filePath)) ) {

                        Map<Long, List<String>> map = new TreeMap<>();

                        wr.write(BenchmarkEntry.getCSVHeader());
                        String line;
                        br.readLine();
                        while ((line = br.readLine()) != null) {
                            BenchmarkEntry entry = BenchmarkEntry.fromString(line);

                            List<String> l = map.get(entry.time);
                            if (l == null) {
                                l = new LinkedList<>();
                                map.put(entry.time, l);
                            }
                            l.add(line);
                        }

                        for (List<String> list : map.values()) {
                            for (String val : list) {
                                wr.write(val);
                                wr.write("\n");
                            }
                        }

                        sortedList.add(filePath);
                    }

                }
            }
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

    public void calculateResponses(String resultsPath, ArrayList<String> filePaths) throws IOException {

        System.out.println("Starting calculating response time");

        File file = new File(BenchmarkHelper.directoryPath + resultsPath);

        if (file.mkdirs() || file.exists()) {

            for (String path : filePaths) {

                String filePath =  path.replaceFirst("sortedClients", resultsPath).replaceFirst(".csv", "") + "-results.csv";

                try (BufferedReader br = new BufferedReader(new FileReader(path));
                     BufferedWriter wr = new BufferedWriter(new FileWriter(filePath)) ) {

                    List<BenchmarkEntry> entryReqs = new ArrayList<>();

                    wr.write(BenchmarkEntry.getCSVHeader());
                    String line;
                    br.readLine();
                    while ((line = br.readLine()) != null) {
                        BenchmarkEntry entry = BenchmarkEntry.fromString(line);
                        Boolean matchFound = false;
                        if (entry.name == null) {
                            throw new RuntimeException("Nulls not supported");
                        }
                        if (entry.name.equals("PUBLISH_RECEIVED")) {
                            continue;
                        }

                        if(entryReqs.size() == 0) {
                            entryReqs.add(entry);
                            continue;
                        }

                        for (Iterator<BenchmarkEntry> iterator = entryReqs.iterator(); iterator.hasNext(); ) {
                            BenchmarkEntry entryReq = iterator.next();
                            if (!matchFound && ControlPacketType.valueOf(entry.name) == mapMessageTypes(ControlPacketType.valueOf(entryReq.name)) &&
                            entry.messageId.equals(entryReq.messageId)) {
                                try {
                                    entry.name = entryReq.name;
                                    entry.time = entry.time - entryReq.time;
                                    wr.write(entry.toString());
                                    filePathsResults.add(filePath);
                                    iterator.remove();
                                    matchFound = true;
                                    continue;
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                        if(!matchFound){
                            entryReqs.add(entry);
                        }

                    }

                }
            }
        }
    }

    public void writeStatisticsForFile(String filePath) throws IOException {
        System.out.println("Starting calculating statistics");
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

        helper.sortIntoFiles("", "/clients/", filePathsClients, SortType.clientName);

        helper.sortByMessageTime("/clients", "/sortedClients", filePathsSortedClients);

        helper.calculateResponses( "/clientResults/", filePathsSortedClients);

        helper.sortIntoFiles("clientResults", "/stats/", filePathsMessageTypes, SortType.name);

        for (String path : helper.filePathsMessageTypes) {
            helper.writeStatisticsForFile(path);
        }
    }

}
