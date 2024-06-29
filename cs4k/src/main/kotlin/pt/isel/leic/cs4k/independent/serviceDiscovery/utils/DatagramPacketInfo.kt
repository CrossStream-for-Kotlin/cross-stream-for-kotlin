package pt.isel.leic.cs4k.independent.serviceDiscovery.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule

/**
 * Represents the payload of datagram packets exchanges between neighbors.
 *
 * @property port The port number.
 */
data class DatagramPacketInfo(
    val port: Int
) {

    companion object {
        // ObjectMapper instance for serializing and deserializing JSON.
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        /**
         * Serialize a datagram packet info to JSON string.
         *
         * @param datagramPacketInfo The datagram packet info to serialize.
         * @return The resulting JSON string.
         */
        fun serialize(datagramPacketInfo: DatagramPacketInfo): String = objectMapper.writeValueAsString(datagramPacketInfo)

        /**
         * Deserialize a JSON string to datagram packet info.
         *
         * @param json The JSON string to deserialize.
         * @return The resulting datagram packet info.
         */
        fun deserialize(json: String): DatagramPacketInfo = objectMapper.readValue(json, DatagramPacketInfo::class.java)
    }
}
