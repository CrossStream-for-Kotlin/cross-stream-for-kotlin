package pt.isel.leic.cs4k.rabbitmq.historyShare

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import pt.isel.leic.cs4k.rabbitmq.ConsumedTopics
import pt.isel.leic.cs4k.rabbitmq.historyShare.HistoryShareMessage.HistoryShareMessageType.RESPONSE

/**
 * Represents a response to a request previously made.
 *
 * @property allConsumeInfo A list of all offsets and latest events of a respective topic.
 */
data class HistoryShareResponse(
    val allConsumeInfo: List<ConsumedTopics.ConsumeInfo>
) {

    /**
     * Converts the response into a message.
     *
     * @return A message containing the response.
     */
    fun toHistoryShareMessage() = HistoryShareMessage(RESPONSE, serialize(this))

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        fun serialize(value: HistoryShareResponse): String = objectMapper.writeValueAsString(value)

        fun deserialize(string: String): HistoryShareResponse = objectMapper.readValue(string, HistoryShareResponse::class.java)
    }
}
