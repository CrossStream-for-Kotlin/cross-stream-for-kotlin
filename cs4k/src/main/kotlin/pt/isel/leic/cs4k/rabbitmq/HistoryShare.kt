package pt.isel.leic.cs4k.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import pt.isel.leic.cs4k.rabbitmq.HistoryShareMessage.HistoryShareMessageType.REQUEST
import pt.isel.leic.cs4k.rabbitmq.HistoryShareMessage.HistoryShareMessageType.RESPONSE

/**
 * A message meant for history sharing.
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
        fun serialize(value: HistoryShareMessage) = objectMapper.writeValueAsString(value)
        fun deserialize(string: String) = objectMapper.readValue(string, HistoryShareMessage::class.java)
    }
}

/**
 * Represents a request for history sharing,
 * @property senderQueue The requesting broker's queue, where the response should be sent to.
 */
data class HistoryShareRequest(
    val senderQueue: String
) {
    /**
     * Converts the request into a message.
     * @return A message containing the request.
     */
    fun toHistoryShareMessage() = HistoryShareMessage(REQUEST, serialize(this))

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())
        fun serialize(value: HistoryShareRequest) = objectMapper.writeValueAsString(value)
        fun deserialize(string: String) = objectMapper.readValue(string, HistoryShareRequest::class.java)
    }
}

/**
 * Represents a response to a request previously made.
 * @property allConsumeInfo A list of all offsets and latest events of a respective topic.
 */
data class HistoryShareResponse(
    val allConsumeInfo: List<ConsumedTopics.ConsumeInfo>
) {

    /**
     * Converts the response into a message.
     * @return A message containing the response.
     */
    fun toHistoryShareMessage() = HistoryShareMessage(RESPONSE, serialize(this))

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())
        fun serialize(value: HistoryShareResponse) = objectMapper.writeValueAsString(value)
        fun deserialize(string: String) = objectMapper.readValue(string, HistoryShareResponse::class.java)
    }
}
