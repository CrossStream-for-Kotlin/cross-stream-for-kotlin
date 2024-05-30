package pt.isel.leic.cs4k.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule

data class Message(
    val topic: String,
    val message: String,
    val isLast: Boolean
) {
    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        fun serialize(message: Message): String = objectMapper.writeValueAsString(message)

        /**
         * Converting the string format into the object.
         * @param value The string form of the request.
         * @return The request object.
         */
        fun deserialize(value: String): Message =
            objectMapper.readValue(value, Message::class.java)
    }
}
