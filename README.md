# Master Thesis from Konstantinos Chaitas
 
## Title: Design and Implementation of a Scalable, Distributed, Location-Based Pub/Sub System

This project is used to evaluate the [DistributedGeoBroker](https://github.com/CostasChaitas/DistributedGeobroker) project

This project/research is based on the [Geobroker project](https://github.com/MoeweX/geobroker) from Jonathan Hasenburg.

## Installation

This is a Java, Kotlin and Maven project. Please install Java 8, Kotlin 1.3.x and Maven 3.6.x

```
git clone https://github.com/CostasChaitas/DistributedGeobrokerEvaluation.git
cd DistributedGeobrokerEvaluation
mvn clean install
```

## Generate Test Data

There are 4 different scenarios that generate test data for the benchmark and can be found under the folder /TestGenerators. Read the description in the README.md files for more information.

In order to generate test data, firstly you have to run the Kotlin Class(es). A new folder with the test data will be created in the top level of this project.


## Run Benchmark

Since the benchmark uses multiple threads and open multiple TCP socket connections, some system-OS configuration has to happen. Run the bash script using:
```
sudo ./sock-config.sh
```

In order to run the benchmark, we have to specify the folder name which contains the test data and also the URL of the API. This can be found on the very top of the class called **Benchmark**. Please adjust the following variables:
```
private final String testsDirectoryPath = "./{NameHere}/";
private final String apiURL = "ws://{HostnameHere:PortHere}";
```

Finally run the **Benchmark** class. A folder called `benchmarking_results` will be created containing the results of the benchmark.

## Generate Benchmark Statistics

In order to generate some useful statistics from the results of the benchmark, like median or the mean request/response time, you can run the **BenchmarHelper** class. This will generate some extra files in the `benchmarking_results` folder with some useful statistics.
 



