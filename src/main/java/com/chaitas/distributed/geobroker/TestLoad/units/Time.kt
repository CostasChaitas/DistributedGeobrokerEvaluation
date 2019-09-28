package com.chaitas.distributed.geobroker.TestLoad.units

import kotlin.math.pow

class Time(private val time: Double, private val unit: Unit) {

    constructor(time: Int, unit: Unit) : this(time.toDouble(), unit)

    enum class Unit {
        MS, S, MIN, H
    }

    /**
     * Depicts the factor of [time] related to the internal default unit SECONDS.
     */
    private val factor = when (unit) {
        Unit.MS -> 10.0.pow(-3)
        Unit.S -> 1.0
        Unit.MIN -> 60.0
        Unit.H -> 60.0 * 60.0
    }

    fun d(targetUnit: Unit): Double {
        return when (targetUnit) {
            Unit.MS -> (time * factor * 10.0.pow(3))
            Unit.S -> (time * factor)
            Unit.MIN -> (time * factor / 60.0)
            Unit.H -> (time * factor / 60.0 / 60.0)
        }
    }

    fun i(targetUnit: Unit): Int {
        return d(targetUnit).toInt()
    }

    operator fun plus(otherTime: Time): Time {
        // pick smaller unit as new internal unit
        val newUnit = if (unit < otherTime.unit) unit else otherTime.unit

        val e1 = d(newUnit)
        val e2 = otherTime.d(newUnit)

        return Time(e1 + e2, newUnit)
    }

    operator fun compareTo(otherTime: Time): Int {
        // idea: compare smallest time unit
        return d(Unit.MS).compareTo(otherTime.d(Unit.MS))
    }

    override fun toString(): String {
        return "~${i(Unit.S)}s"
    }

}