package pt.isel.leic.cs4k.utils

/**
 * Responsible for accessing environment variables.
 */
object Environment {

    // Name of environment variable for PostgreSQL database URL.
    private const val KEY_POSTGRESQL_DB_URL = "POSTGRESQL_DB_URL"

    // Name of environment variable for Redis host.
    private const val KEY_REDIS_HOST = "REDIS_HOST"

    // Name of environment variable for Redis port.
    private const val KEY_REDIS_PORT = "REDIS_PORT"

    /**
     * Get the PostgreSQL database URL from the environment variable [KEY_POSTGRESQL_DB_URL].
     *
     * @return The PostgreSQL database URL.
     * @throws Exception If the environment variable for the PostgreSQL database URL is missing.
     */
    fun getPostgreSQLDbUrl() = System.getenv(KEY_POSTGRESQL_DB_URL)
        ?: throw Exception("Missing environment variable $KEY_POSTGRESQL_DB_URL.")

    /**
     * Get the Redis host from the environment variable [KEY_REDIS_HOST].
     *
     * @return Redis host name.
     * @throws Exception If the environment variable for the Redis host is missing.
     */
    fun getRedisHost() = System.getenv(KEY_REDIS_HOST)
        ?: throw Exception("Missing environment variable $KEY_REDIS_HOST.")

    /**
     * Get the Redis port from the environment variable [KEY_REDIS_PORT].
     *
     * @return Redis port number.
     * @throws Exception If the environment variable for the Redis port is missing.
     */
    fun getRedisPort() = System.getenv(KEY_REDIS_PORT)?.toInt()
        ?: throw Exception("Missing environment variable $KEY_REDIS_PORT.")
}
