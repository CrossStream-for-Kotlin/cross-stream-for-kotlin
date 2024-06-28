package pt.isel.leic.cs4k.rabbitmq

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class MessageTests {

    @Test
    fun `serialize message to json and then deserialize it again to message and check if it is equal`() {
        // Arrange
        // Act
        val serializedMessage = Message.serialize(message)
        val deserializedMessage = Message.deserialize(serializedMessage)

        // Assert
        assertEquals(message, deserializedMessage)
    }

    private companion object {
        private val message = Message(
            topic = "topic",
            message = "message",
            isLast = true
        )
    }
}
