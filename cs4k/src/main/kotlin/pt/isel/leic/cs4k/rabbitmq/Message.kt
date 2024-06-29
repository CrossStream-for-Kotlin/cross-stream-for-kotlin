package pt.isel.leic.cs4k.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule

/**
 * Represents a message published to Rabbit Stream.
 *
 * @property topic The topic of the event.
 * @property message The message of the event.
 * @property isLast If the event is the last one.
 */
data class Message(
    val topic: String,
    val message: String,
    val isLast: Boolean = false
) {

    companion object {
        // ObjectMapper instance for serializing and deserializing JSON.
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        /**
         * Serialize a message to JSON string.
         *
         * @param message The message to serialize.
         * @return The resulting JSON string.
         */
        fun serialize(message: Message): String = objectMapper.writeValueAsString(message)

        /**
         * Deserialize a JSON string to message.
         *
         * @param value The JSON string to deserialize.
         * @return The resulting message.
         */
        fun deserialize(value: String): Message = objectMapper.readValue(value, Message::class.java)
    }
}
