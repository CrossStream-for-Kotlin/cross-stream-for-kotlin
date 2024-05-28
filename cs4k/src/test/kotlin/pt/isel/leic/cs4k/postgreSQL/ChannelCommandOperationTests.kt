package pt.isel.leic.cs4k.postgreSQL

import kotlin.test.Test
import kotlin.test.assertEquals

class ChannelCommandOperationTests {

    @Test
    fun `check 'Listen' toString override`() {
        // Arrange
        // Act
        // Assert
        assertEquals("listen", "${ChannelCommandOperation.Listen}")
    }

    @Test
    fun `check 'UnListen' toString override`() {
        // Arrange
        // Act
        // Assert
        assertEquals("unlisten", "${ChannelCommandOperation.UnListen}")
    }
}
