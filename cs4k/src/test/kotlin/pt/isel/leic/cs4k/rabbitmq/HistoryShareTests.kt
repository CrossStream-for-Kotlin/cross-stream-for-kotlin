package pt.isel.leic.cs4k.rabbitmq

import pt.isel.leic.cs4k.common.Event
import pt.isel.leic.cs4k.rabbitmq.historyShare.HistoryShareMessage
import pt.isel.leic.cs4k.rabbitmq.historyShare.HistoryShareRequest
import pt.isel.leic.cs4k.rabbitmq.historyShare.HistoryShareResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class HistoryShareTests {

    @Test
    fun `request can be converted to message and back`() {
        val request = HistoryShareRequest("queue")
        val message = request.toHistoryShareMessage()
        assertTrue { message.type == HistoryShareMessage.HistoryShareMessageType.REQUEST }
        val requestFromMessage = message.toRequest()
        assertEquals(request, requestFromMessage)
    }

    @Test
    fun `response can be converted to message and back`() {
        val event = Event("topic", 0, "message")
        val response = HistoryShareResponse(listOf(ConsumedTopics.ConsumeInfo(3L, event)))
        val message = response.toHistoryShareMessage()
        assertTrue { message.type == HistoryShareMessage.HistoryShareMessageType.RESPONSE }
        val responseFromMessage = message.toResponse()
        assertEquals(response, responseFromMessage)
    }

    @Test
    fun `request cannot be converted to response`() {
        val request = HistoryShareRequest("queue")
        val message = request.toHistoryShareMessage()
        assertTrue { message.type == HistoryShareMessage.HistoryShareMessageType.REQUEST }
        assertFailsWith<IllegalArgumentException> {
            message.toResponse()
        }
    }

    @Test
    fun `response cannot be converted to request`() {
        val event = Event("topic", 0, "message")
        val response = HistoryShareResponse(listOf(ConsumedTopics.ConsumeInfo(3L, event)))
        val message = response.toHistoryShareMessage()
        assertTrue { message.type == HistoryShareMessage.HistoryShareMessageType.RESPONSE }
        assertFailsWith<IllegalArgumentException> {
            message.toRequest()
        }
    }
}
