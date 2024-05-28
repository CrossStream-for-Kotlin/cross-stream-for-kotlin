package pt.isel.leic.cs4k.common

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import pt.isel.leic.cs4k.common.BrokerException.ConnectionPoolSizeException

class UtilsTests {

    @Test
    fun `not thrown an exception with a acceptable value for the connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertDoesNotThrow {
            Utils.checkDbConnectionPoolSize(ACCEPTABLE_CONNECTION_POOL_SIZE)
        }
    }

    @Test
    fun `thrown an exception with a big value for the connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertThrows(ConnectionPoolSizeException::class.java) {
            Utils.checkDbConnectionPoolSize(BIG_CONNECTION_POOL_SIZE)
        }
    }

    @Test
    fun `thrown an exception with a negative value for the connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertThrows(ConnectionPoolSizeException::class.java) {
            Utils.checkDbConnectionPoolSize(NEGATIVE_CONNECTION_POOL_SIZE)
        }
    }

    companion object {
        private const val ACCEPTABLE_CONNECTION_POOL_SIZE = 10
        private const val BIG_CONNECTION_POOL_SIZE = 10_000
        private const val NEGATIVE_CONNECTION_POOL_SIZE = -10_000
    }
}
