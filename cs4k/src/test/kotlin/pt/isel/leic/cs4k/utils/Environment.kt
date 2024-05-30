package pt.isel.leic.cs4k.utils

/**
 * Responsible for accessing environment variables.
 */
object Environment {

    // Keys for every system variable used.
    private const val KEY_POSTGRESQL_DB_URL = "POSTGRESQL_DB_URL"
    private const val KEY_REDIS_USR = "REDIS_USR"
    private const val KEY_REDIS_PSW = "REDIS_PSW"
    private const val KEY_REDIS_ADDRESS = "REDIS_ADDRESS"
    private const val KEY_RABBIT_USR = "KEY_RABBIT_USR"
    private const val KEY_RABBIT_PSW = "KEY_RABBIT_PSW"
    private const val KEY_RABBIT_ADDRESS = "KEY_RABBIT_ADDRESS"

    /**
     * Obtains a system variable with a given key.
     * @param key An environment variable key.
     * @throws NullPointerException If no environment variable was found.
     */
    private fun getEnvVar(key: String) = System.getenv(key)
        ?: throw NullPointerException("Missing environment variable $key.")

    /**
     * Obtains the URL to access PostgreSQL database.
     */
    fun getPostgreSqlDbUrl() = getEnvVar(KEY_POSTGRESQL_DB_URL)

    /**
     * Obtains the username to access Redis database.
     */
    fun getRedisUser() = getEnvVar(KEY_REDIS_USR)

    /**
     * Obtains the password to access Redis database.
     */
    fun getRedisPassword() = getEnvVar(KEY_REDIS_PSW)

    /**
     * Obtains the address of the Redis database.
     */
    fun getRedisAddress() = getEnvVar(KEY_REDIS_ADDRESS)

    /**
     * Obtains the host name of the Redis database.
     */
    fun getRedisHost() = getRedisAddress().split(":").first()

    /**
     * Obtains the port number of the Redis database.
     */
    fun getRedisPort() = getRedisAddress().split(":").last().toInt()

    /**
     * Obtains the username to access the RabbitMQ broker.
     */
    fun getRabbitUser() = getEnvVar(KEY_RABBIT_USR)

    /**
     * Obtains the password to access the RabbitMQ broker.
     */
    fun getRabbitPassword() = getEnvVar(KEY_RABBIT_PSW)

    /**
     * Obtains the address of the RabbitMQ broker.
     */
    fun getRabbitAddress() = getEnvVar(KEY_RABBIT_ADDRESS)

    /**
     * Obtains the host name of the RabbitMQ broker.
     */
    fun getRabbitHost() = getRabbitAddress().split(":").first()

    /**
     * Obtains the port number of the RabbitMQ broker.
     */
    fun getRabbitPort() = getRabbitAddress().split(":").last().toInt()

}
