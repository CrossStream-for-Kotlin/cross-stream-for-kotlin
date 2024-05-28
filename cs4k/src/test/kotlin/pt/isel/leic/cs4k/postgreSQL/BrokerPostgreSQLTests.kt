package pt.isel.leic.cs4k.postgreSQL

import org.junit.jupiter.api.assertDoesNotThrow
import pt.isel.leic.cs4k.common.BrokerException
import pt.isel.leic.cs4k.utils.Environment
import kotlin.test.Test
import kotlin.test.assertFailsWith

class BrokerPostgreSQLTests {

    @Test
    fun `can create a BrokerSQL with a acceptable database connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertDoesNotThrow {
            BrokerPostgreSQL(
                Environment.getPostgreSQLDbUrl(),
                dbConnectionPoolSize = ACCEPTABLE_CONNECTION_POOL_SIZE
            )
        }
    }

    @Test
    fun `cannot create a BrokerSQL with a big database connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertFailsWith<BrokerException.ConnectionPoolSizeException> {
            BrokerPostgreSQL(
                Environment.getPostgreSQLDbUrl(),
                dbConnectionPoolSize = BIG_CONNECTION_POOL_SIZE
            )
        }
    }

    @Test
    fun `cannot create a BrokerSQL with a negative database connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertFailsWith<BrokerException.ConnectionPoolSizeException> {
            BrokerPostgreSQL(
                Environment.getPostgreSQLDbUrl(),
                dbConnectionPoolSize = NEGATIVE_CONNECTION_POOL_SIZE
            )
        }
    }

    companion object {
        private const val ACCEPTABLE_CONNECTION_POOL_SIZE = 10
        private const val BIG_CONNECTION_POOL_SIZE = 10_000
        private const val NEGATIVE_CONNECTION_POOL_SIZE = -10_000
    }
}
