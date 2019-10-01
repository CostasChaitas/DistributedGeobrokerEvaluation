// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.TestGenerators.units

class Distance(private val distance: Double, private val unit: Unit) {

    constructor(distance: Int, unit: Unit) : this(distance.toDouble(), unit)

    enum class Unit {
        M, KM
    }

    /**
     * Depicts the factor of [distance] related to the internal default unit METER.
     */
    private val factor = when (unit) {
        Unit.M -> 1.0
        Unit.KM -> 1000.0
    }

    fun d(targetUnit: Unit): Double {
        return when (targetUnit) {
            Unit.M -> (distance * factor)
            Unit.KM -> (distance * factor / 1000.0)
        }
    }

    fun i(targetUnit: Unit): Int {
        return d(targetUnit).toInt()
    }

    operator fun plus(otherDistance: Distance): Distance {
        // pick smaller unit as new internal unit
        val newUnit = if (unit < otherDistance.unit) unit else otherDistance.unit

        val e1 = d(newUnit)
        val e2 = otherDistance.d(newUnit)

        return Distance(e1 + e2, newUnit)
    }

    operator fun compareTo(otherDistance: Distance): Int {
        // idea: compare smallest time unit
        return d(Unit.M).compareTo(otherDistance.d(Unit.M))
    }

    override fun toString(): String {
        return "~${i(Unit.M)}m"
    }

}