package pt.isel.leic.cs4k.common

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule

/**
 * Responsible for the serialization and deserialization process.
 */
object BrokerSerializer {

    // ObjectMapper instance for serializing and deserializing JSON.
    private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

    /**
     * Serialize an event to JSON string.
     *
     * @param event The event to serialize.
     * @return The resulting JSON string.
     */
    fun serializeEventToJson(event: Event): String = objectMapper.writeValueAsString(event)

    /**
     * Deserialize a JSON string to event.
     *
     * @param json The JSON string to deserialize.
     * @return The resulting event.
     */
    fun deserializeEventFromJson(json: String): Event = objectMapper.readValue(json, Event::class.java)
}
