package pt.isel.leic.cs4k.common

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class BrokerSerializerTests {

    @Test
    fun `serialize event to json and then deserialize it again to event and check if it is equal`() {
        // Arrange
        // Act
        val serializedEvent = BrokerSerializer.serializeEventToJson(event)
        val deserializedEvent = BrokerSerializer.deserializeEventFromJson(serializedEvent)

        // Assert
        assertEquals(event, deserializedEvent)
    }

    private companion object {
        private val event = Event(
            topic = "topic",
            id = 0,
            message = "message",
            isLast = true
        )
    }
}
