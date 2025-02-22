// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.TestGenerators

import com.chaitas.distributed.geobroker.TestGenerators.units.Distance
import com.chaitas.distributed.geobroker.TestGenerators.units.Distance.Unit.KM
import com.chaitas.distributed.geobroker.TestGenerators.units.Time
import com.chaitas.distributed.geobroker.TestGenerators.units.Time.Unit.H
import com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.Geofence


class Stats {
    private var numberOfPingMessages = 0
    private var clientDistanceTravelled = Distance(0, Distance.Unit.M)
    private var numberOfPublishedMessages = 0
    private var totalPayloadSize = 0
    private var numberOfOverlappingSubscriptionGeofences = 0
    private var numberOfSubscribeMessages = 0
    private var numberOfOverlappingMessageGeofences = 0

    /*****************************************************************
     * Get Stats
     ****************************************************************/

    fun getNumberOfPingMessages(): Int {
        return numberOfPingMessages
    }

    fun getNumberOfPublishedMessages(): Int {
        return numberOfPublishedMessages
    }

    fun getNumberOfSubscribeMessages(): Int {
        return numberOfSubscribeMessages
    }

    fun getClientDistanceTravelled(): Distance {
        return clientDistanceTravelled
    }

    fun getNumberOfOverlappingSubscriptionGeofences(): Int {
        return numberOfOverlappingSubscriptionGeofences
    }

    fun getNumberOfOverlappingMessageGeofences(): Int {
        return numberOfOverlappingMessageGeofences
    }

    fun getTotalPayloadSize(): Int {
        return totalPayloadSize
    }

    /*****************************************************************
     * Add Stats
     ****************************************************************/

    fun addPingMessage() {
        numberOfPingMessages++
    }

    fun addSubscribeMessage() {
        numberOfSubscribeMessages++
    }

    fun addPublishMessage() {
        numberOfPublishedMessages++
    }

    fun addPayloadSize(size: Int) {
        totalPayloadSize += size
    }

    /**
     * @param distance - distance travelled by the client
     */
    fun addClientDistanceTravelled(distance: Distance) {
        clientDistanceTravelled += distance
    }

    /**
     * Counts and stores the number of overlapping subscription geofences.
     *
     */
    fun addSubscriptionGeofenceOverlaps(geofence: Geofence, brokerAreas: List<Geofence>) {
        var intersects = -1 // own broker
        brokerAreas.forEach {
            if (geofence.intersects(it)) {
                intersects++
            }
        }
        numberOfOverlappingSubscriptionGeofences += intersects
    }

    /**
     * Counts and stores the number of overlapping message geofences.
     *
     */
    fun addMessageGeofenceOverlaps(geofence: Geofence, brokerAreas: List<Geofence>) {
        var intersects = -1 // own broker
        brokerAreas.forEach {
            if (geofence.intersects(it)) {
                intersects++
            }
        }
        numberOfOverlappingMessageGeofences += intersects
    }

    /*****************************************************************
     * Summary
     ****************************************************************/

    fun getSummary(subsPerBrokerArea: List<Int>, pubsPerBrokerArea: List<Int>, timeToRunPerClient: Time): String {
        val subscribers = subsPerBrokerArea.stream().mapToInt { it }.sum()
        val publishers = pubsPerBrokerArea.stream().mapToInt { it }.sum()
        val clients = subscribers + publishers

        return getSummary(clients, timeToRunPerClient)
    }

    fun getSummary(clientsPerBrokerArea: List<Int>, timeToRunPerClient: Time): String {
        val clients = clientsPerBrokerArea.stream().mapToInt { it }.sum()

        return getSummary(clients, timeToRunPerClient)
    }

    @Suppress("LocalVariableName")
    private fun getSummary(numberOfClients: Int, timeToRunPerClient: Time): String {
        val distancePerClient_KM = getClientDistanceTravelled().d(KM) / numberOfClients // km
        val runtime_S = timeToRunPerClient.d(Time.Unit.S) //s

        return """
Data set characteristics:
    Number of ping messages: ${getNumberOfPingMessages()} (${getNumberOfPingMessages() / runtime_S} messages/s)
    Number of subscribe messages: ${getNumberOfSubscribeMessages()} (${getNumberOfSubscribeMessages() / runtime_S} messages/s)
    Number of publish messages: ${getNumberOfPublishedMessages()} (${getNumberOfPublishedMessages() / runtime_S} messages/s)
    Publish payload size: ${getTotalPayloadSize() / 1000.0}KB (${getTotalPayloadSize() / getNumberOfPublishedMessages()} bytes/message)
    Client distance travelled: ${getClientDistanceTravelled().d(KM)}km ($distancePerClient_KM km/client)
    Client average speed: ${distancePerClient_KM / timeToRunPerClient.d(H)} km/h
    Number of message geofence broker overlaps: ${getNumberOfOverlappingMessageGeofences()}
    Number of subscription geofence broker overlaps: ${getNumberOfOverlappingSubscriptionGeofences()}
"""
    }


}