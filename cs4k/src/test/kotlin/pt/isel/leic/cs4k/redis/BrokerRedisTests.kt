package pt.isel.leic.cs4k.redis

import org.junit.jupiter.api.assertDoesNotThrow
import pt.isel.leic.cs4k.common.BrokerException
import pt.isel.leic.cs4k.utils.Environment
import kotlin.test.Test
import kotlin.test.assertFailsWith

class BrokerRedisTests {

    @Test
    fun `can create a BrokerRedis with a acceptable database connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertDoesNotThrow {
            BrokerRedis(
                RedisNode(Environment.getRedisHost(), Environment.getRedisPort()),
                dbConnectionPoolSize = ACCEPTABLE_CONNECTION_POOL_SIZE
            )
        }
    }

    @Test
    fun `cannot create a BrokerRedis with a big database connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertFailsWith<BrokerException.ConnectionPoolSizeException> {
            BrokerRedis(
                RedisNode(Environment.getRedisHost(), Environment.getRedisPort()),
                dbConnectionPoolSize = BIG_CONNECTION_POOL_SIZE
            )
        }
    }

    @Test
    fun `cannot create a BrokerRedis with a negative database connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertFailsWith<BrokerException.ConnectionPoolSizeException> {
            BrokerRedis(
                RedisNode(Environment.getRedisHost(), Environment.getRedisPort()),
                dbConnectionPoolSize = NEGATIVE_CONNECTION_POOL_SIZE
            )
        }
    }

    @Test
    fun `cannot create a BrokerRedis with a empty list of nodes`() {
        // Arrange
        // Act
        // Assert
        assertFailsWith<BrokerException.NodeListIsEmptyException> {
            BrokerRedis(
                emptyList<RedisNode>()
            )
        }
    }

    companion object {
        private const val ACCEPTABLE_CONNECTION_POOL_SIZE = 10
        private const val BIG_CONNECTION_POOL_SIZE = 10_000
        private const val NEGATIVE_CONNECTION_POOL_SIZE = -10_000
    }
}
