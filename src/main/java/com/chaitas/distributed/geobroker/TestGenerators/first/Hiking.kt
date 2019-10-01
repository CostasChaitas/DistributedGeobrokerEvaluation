// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.TestGenerators.first

import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Geofence
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Location
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.randomName
import com.chaitas.distributed.geobroker.TestGenerators.*
import com.chaitas.distributed.geobroker.TestGenerators.units.Distance
import com.chaitas.distributed.geobroker.TestGenerators.units.Time
import com.chaitas.distributed.geobroker.TestGenerators.units.Time.Unit.*
import org.locationtech.spatial4j.distance.DistanceUtils.KM_TO_DEG
import java.io.File
import kotlin.random.Random


// -------- Travel --------
private const val minTravelSpeed = 2 // km/h
private const val maxTravelSpeed = 8 // km/h
private val minTravelTime = Time(5, S)
private val maxTravelTime = Time(30, S)

// -------- Brokers --------
private val brokerNames = listOf("Columbus", "Frankfurt", "Paris")
private val brokerAreas = listOf(Geofence.circle(Location(39.961332, -82.999083), 5.0),
        Geofence.circle(Location(50.106732, 8.663124), 2.1),
        Geofence.circle(Location(48.877366, 2.359708), 2.1))
// to split the workload evenly across multiple machines for a given broker
private val workloadMachinesPerBroker = listOf(3, 3, 3)
private val clientsPerBrokerArea = listOf(1200, 1200, 1200)

// -------- Geofences -------- values are in degree
private const val roadConditionSubscriptionGeofenceDiameter = 0.5 * KM_TO_DEG
private const val roadConditionMessageGeofenceDiameter = 0.5 * KM_TO_DEG
private const val minTextBroadcastSubscriptionGeofenceDiameter = 1.0 * KM_TO_DEG
private const val maxTextBroadcastSubscriptionGeofenceDiameter = 50.0 * KM_TO_DEG
private const val minTextBroadcastMessageGeofenceDiameter = 1.0 * KM_TO_DEG
private const val maxTextBroadcastMessageGeofenceDiameter = 50.0 * KM_TO_DEG

// -------- Messages  --------
private const val roadConditionPublicationProbability = 10 // %
private const val textBroadcastPublicationProbability = 50 // %
private const val roadConditionPayloadSize = 100 // byte
private const val minTextBroadcastPayloadSize = 10 // byte
private const val maxTextBroadcastPayloadSize = 1000 // byte

// -------- Others  --------
private const val directoryPath = "./hiking"
private const val roadConditionTopic = "road"
private const val textBroadcastTopic = "text"
private val subscriptionRenewalDistance = Distance(50, Distance.Unit.M)
private val warmupTime = Time(5, S)
private val timeToRunPerClient = Time(15, MIN)

/**
 * Steps:
 *  1. pick broker
 *  2. pick a client -> random name
 *  3. loop:
 *    - calculate next location and timestamp
 *    - send: ping, subscribe, publish
 */
fun main(args: Array<String>) {
    validateBrokersDoNotOverlap(brokerAreas)
    prepareDir(directoryPath)

    val stats = Stats()
    val setup = getSetupString("com.chaitas.distributed.geobroker.TestGenerators.first.HikingKt")
    System.out.println(setup)
    File("$directoryPath/00_summary.txt").writeText(setup)

    for (b in 0..2) {
        // pick a broker
        val broker = getBrokerTriple(b, brokerNames, brokerAreas, clientsPerBrokerArea)

        System.out.println("Calculating actions for broker ${broker.first}")

        var currentWorkloadMachine: Int

        // loop through clients for broker
        for (c in 1..broker.third) {
            currentWorkloadMachine = getCurrentWorkloadMachine(c, broker.first, workloadMachinesPerBroker[b], broker.third)
            val clientName = randomName()
            val clientDirection = Random.nextDouble(0.0, 360.0)
            System.out.println("Calculating actions for client $clientName which travels in $clientDirection")

            // file and writer
            val file = File("$directoryPath/${broker.first}-${currentWorkloadMachine}_$clientName.csv")
            val writer = file.bufferedWriter()
            writer.write(getHeader())

            // vars
            var location = Location.randomInGeofence(broker.second)
            var lastUpdatedLocation = location // needed to determine if subscription should be updated
            var timestamp = Time(Random.nextInt(0, warmupTime.i(MS)), MS)

            // this geofence is only calculated once per client
            val geofenceTB = Geofence.circle(location,
                    Random.nextDouble(minTextBroadcastSubscriptionGeofenceDiameter,
                            maxTextBroadcastSubscriptionGeofenceDiameter))

            // send first ping and create initial subscriptions
            writer.write(calculatePingAction(timestamp, location, stats))
            writer.write(calculateSubscribeActions(timestamp, location, geofenceTB, stats))
            timestamp = warmupTime

            // generate actions until time reached
            while (timestamp <= timeToRunPerClient) {
                writer.write(calculatePingAction(timestamp, location, stats))
                val travelledDistance = Distance(location.distanceKmTo(lastUpdatedLocation), Distance.Unit.KM)
                if (travelledDistance >= subscriptionRenewalDistance) {
                    System.out.println("Renewing subscription for client $clientName")
                    writer.write(calculateSubscribeActions(timestamp, location, geofenceTB, stats))
                    lastUpdatedLocation = location
                }
                writer.write(calculatePublishActions(timestamp, location, stats))

                val travelTime = Time(Random.nextInt(minTravelTime.i(MS), maxTravelTime.i(MS)), MS)
                location = calculateNextLocation(broker.second,
                        location,
                        clientDirection,
                        travelTime,
                        minTravelSpeed,
                        maxTravelSpeed,
                        stats)
                timestamp += travelTime
            }

            // add a last ping message at runtime, as "last message"
            writer.write(calculatePingAction(timeToRunPerClient, location, stats))

            writer.flush()
            writer.close()
        }
    }
    val output = stats.getSummary(clientsPerBrokerArea, timeToRunPerClient)
    System.out.println(output)
    File("$directoryPath/00_summary.txt").appendText(output)
}

private fun calculatePingAction(timestamp: Time, location: Location, stats: Stats): String {
    stats.addPingMessage()
    return "${timestamp.i(MS)};${location.lat};${location.lon};ping;;;\n"
}

private fun calculateSubscribeActions(timestamp: Time, location: Location, geofenceTB: Geofence, stats: Stats): String {
    val actions = StringBuilder()

    // road condition
    val geofenceRC = Geofence.circle(location, roadConditionSubscriptionGeofenceDiameter)
    actions.append("${timestamp.i(MS) + 1};${location.lat};${location.lon};subscribe;" + "$roadConditionTopic;${geofenceRC.wkt};\n")
    stats.addSubscriptionGeofenceOverlaps(geofenceRC, brokerAreas)
    stats.addSubscribeMessage()

    // text broadcast
    actions.append("${timestamp.i(MS) + 2};${location.lat};${location.lon};subscribe;" + "$textBroadcastTopic;${geofenceTB.wkt};\n")
    stats.addSubscriptionGeofenceOverlaps(geofenceTB, brokerAreas)
    stats.addSubscribeMessage()

    return actions.toString()
}

private fun calculatePublishActions(timestamp: Time, location: Location, stats: Stats): String {
    val actions = StringBuilder()

    // road condition
    if (getTrueWithChance(roadConditionPublicationProbability)) {
        val geofenceRC = Geofence.circle(location, roadConditionMessageGeofenceDiameter)
        actions.append("${timestamp.i(MS) + 3};${location.lat};${location.lon};publish;" + "$roadConditionTopic;${geofenceRC.wkt};$roadConditionPayloadSize\n")
        stats.addMessageGeofenceOverlaps(geofenceRC, brokerAreas)
        stats.addPublishMessage()
        stats.addPayloadSize(roadConditionPayloadSize)
    }

    // text broadcast
    if (getTrueWithChance(textBroadcastPublicationProbability)) {
        val geofenceTB = Geofence.circle(location,
                Random.nextDouble(minTextBroadcastMessageGeofenceDiameter, maxTextBroadcastMessageGeofenceDiameter))
        val payloadSize = Random.nextInt(minTextBroadcastPayloadSize, maxTextBroadcastPayloadSize)
        actions.append("${timestamp.i(MS) + 4};${location.lat};${location.lon};publish;" + "$textBroadcastTopic;${geofenceTB.wkt};$payloadSize\n")
        stats.addMessageGeofenceOverlaps(geofenceTB, brokerAreas)
        stats.addPublishMessage()
        stats.addPayloadSize(payloadSize)
    }
    return actions.toString()
}