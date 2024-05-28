package pt.isel.leic.cs4k.common

import pt.isel.leic.cs4k.common.BrokerException.ConnectionPoolSizeException

/**
 * Utility operations required in Broker.
 */
object Utils {

    // Default database connection pool size.
    const val DEFAULT_DB_CONNECTION_POOL_SIZE = 10

    // Minimum database connection pool size allowed.
    private const val MIN_DB_CONNECTION_POOL_SIZE = 2

    // Maximum database connection pool size allowed.
    private const val MAX_DB_CONNECTION_POOL_SIZE = 100

    /**
     * Check if the provided database connection pool size is within the acceptable range.
     *
     * @param dbConnectionPoolSize The size of the database connection pool to check.
     * @throws ConnectionPoolSizeException If the size is outside the acceptable range.
     */
    fun checkDbConnectionPoolSize(dbConnectionPoolSize: Int) {
        if (dbConnectionPoolSize !in MIN_DB_CONNECTION_POOL_SIZE..MAX_DB_CONNECTION_POOL_SIZE) {
            throw ConnectionPoolSizeException(
                "The connection pool size must be between $MIN_DB_CONNECTION_POOL_SIZE and $MAX_DB_CONNECTION_POOL_SIZE."
            )
        }
    }
}
