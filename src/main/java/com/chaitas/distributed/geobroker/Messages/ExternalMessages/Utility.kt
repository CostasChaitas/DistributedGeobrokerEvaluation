// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Messages.ExternalMessages

import me.atrox.haikunator.Haikunator

import java.util.*


private val r = Random()
private var haikunator: Haikunator = Haikunator().setRandom(r)

fun sleep(millis: Long, nanos: Int) {
    try {
        Thread.sleep(millis, nanos)
    } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
        System.out.println("Interrupted my sleep :S -> interrupting!$e")
    }

}

fun sleepNoLog(millis: Long, nanos: Int) {
    try {
        Thread.sleep(millis, nanos)
    } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
    }

}

/**
 * Returns true with the given chance.
 *
 * @param chance - the chance to return true (0 - 100)
 * @return true, if lucky
 */
fun getTrueWithChance(chance: Int): Boolean {
    @Suppress("NAME_SHADOWING") var chance = chance
    // normalize
    if (chance > 100) {
        chance = 100
    } else if (chance < 0) {
        chance = 0
    }
    val random = r.nextInt(100) + 1 // not 0
    return random <= chance
}

/**
 * @param bound - int, must be > 0
 * @return a random int between 0 (inclusive) and bound (exclusive)
 */
fun randomInt(bound: Int): Int {
    return r.nextInt(bound)
}

/**
 * @param - [upper] has to be larger than [lower]
 * @return a random int between [lower] (inclusive) and [upper] (exclusive)
 */
fun randomDouble(lower: Double, upper: Double): Double {
    return lower + (upper - lower) * r.nextDouble()
}

fun randomName(): String {
    return haikunator.haikunate()
}

fun randomName(r: Random): String {
    val h = Haikunator().setRandom(r)
    h.random = r
    return h.haikunate()
}

fun generateClientOrderBackendString(identity: String): String {
    return "inproc://$identity"
}

/**
 * Generates a string payload with the given size, but the minimum size is length(content) + 8.
 */
fun generatePayloadWithSize(payloadSize: Int, content: String): String {
    val stringBuilder = StringBuilder()
    stringBuilder.append(content).append("++++++++")
    for (i in 0 until payloadSize - 8 - content.length) {
        stringBuilder.append("a")
    }
    return stringBuilder.toString()
}
