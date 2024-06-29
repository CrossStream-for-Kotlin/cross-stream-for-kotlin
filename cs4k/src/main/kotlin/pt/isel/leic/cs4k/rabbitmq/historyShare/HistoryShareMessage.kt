package pt.isel.leic.cs4k.rabbitmq.historyShare

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import pt.isel.leic.cs4k.rabbitmq.historyShare.HistoryShareMessage.HistoryShareMessageType.REQUEST
import pt.isel.leic.cs4k.rabbitmq.historyShare.HistoryShareMessage.HistoryShareMessageType.RESPONSE

/**
 * A message meant for history sharing.
 *
 * @property type The message type.
 * @property body The serialized message body.
 */
data class HistoryShareMessage(
    val type: HistoryShareMessageType,
    val body: String
) {
    /**
     * The type of message.
     */
    enum class HistoryShareMessageType { REQUEST, RESPONSE }

    /**
     * Converts the message into a request.
     *
     * @return The request present in the body.
     * @throws IllegalArgumentException If the message does not include a request.
     */
    fun toRequest(): HistoryShareRequest =
        if (this.type == REQUEST) {
            HistoryShareRequest.deserialize(this.body)
        } else {
            throw IllegalArgumentException("Trying to get request out of a response.")
        }

    /**
     * Converts the message into a response.
     *
     * @return The response present in the body,
     * @throws IllegalArgumentException If the message does not include a response.
     */
    fun toResponse(): HistoryShareResponse =
        if (this.type == RESPONSE) {
            HistoryShareResponse.deserialize(this.body)
        } else {
            throw IllegalArgumentException("Trying to get response out of a request.")
        }

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        fun serialize(value: HistoryShareMessage): String = objectMapper.writeValueAsString(value)

        fun deserialize(string: String): HistoryShareMessage = objectMapper.readValue(string, HistoryShareMessage::class.java)
    }
}
