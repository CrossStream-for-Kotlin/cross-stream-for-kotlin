package pt.isel.leic.cs4k.rabbitmq.historyShare

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import pt.isel.leic.cs4k.rabbitmq.historyShare.HistoryShareMessage.HistoryShareMessageType.REQUEST

/**
 * Represents a request for history sharing,
 *
 * @property senderQueue The requesting broker's queue, where the response should be sent to.
 */
data class HistoryShareRequest(
    val senderQueue: String
) {
    /**
     * Converts the request into a message.
     *
     * @return A message containing the request.
     */
    fun toHistoryShareMessage() = HistoryShareMessage(REQUEST, serialize(this))

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        fun serialize(value: HistoryShareRequest): String = objectMapper.writeValueAsString(value)

        fun deserialize(string: String): HistoryShareRequest = objectMapper.readValue(string, HistoryShareRequest::class.java)
    }
}
