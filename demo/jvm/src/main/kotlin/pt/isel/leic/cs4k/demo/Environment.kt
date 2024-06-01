package pt.isel.leic.cs4k.demo

/**
 * Responsible for accessing environment variables.
 */
object Environment {

    // Option for CS4k library.
    const val CS4K_OPTION = "CS4K_OPTION"
    private const val KEY_POSTGRESQL_DB_URL = "POSTGRESQL_DB_URL"
    private const val KEY_REDIS_USR = "REDIS_USR"
    private const val KEY_REDIS_PSW = "REDIS_PSW"
    private const val KEY_REDIS_ADDRESS = "REDIS_ADDRESS"
    private const val KEY_RABBIT_USR = "RABBIT_USR"
    private const val KEY_RABBIT_PSW = "RABBIT_PSW"
    private const val KEY_RABBIT_ADDRESS = "RABBIT_ADDRESS"
    private const val KEY_HOSTNAME = "HOSTNAME"
    private const val KEY_SERVICE_NAME = "SERVICE_NAME"


    /**
     * Obtains a system variable with a given key.
     * @param key An environment variable key.
     * @return The system value, or null if the value does not exist.
     */
    private fun getOptionalEnvVar(key: String) = System.getenv(key)

    /**
     * Obtains a system variable with a given key.
     * @param key An environment variable key.
     * @throws NullPointerException If no environment variable was found.
     * @return The system value.
     */
    private fun getRequiredEnvVar(key: String) = System.getenv(key)
        ?: throw NullPointerException("Missing environment variable $key.")

    /**
     * Get the cs4k library option from the environment variable [CS4K_OPTION].
     * @return The cs4k option or null if the environment variable is missing.
     */
    fun getCS4KOption() = getOptionalEnvVar(CS4K_OPTION)?.toFloat()

    /**
     * Obtains the URL to access PostgreSQL database.
     * @return The PostgreSQL connection URL.
     */
    fun getPostgreSqlDbUrl() = getRequiredEnvVar(KEY_POSTGRESQL_DB_URL)

    /**
     * Obtains the username to access Redis database.
     * @return The Redis username.
     */
    fun getRedisUser() = getRequiredEnvVar(KEY_REDIS_USR)

    /**
     * Obtains the password to access Redis database.
     * @return The Redis password.
     */
    fun getRedisPassword() = getRequiredEnvVar(KEY_REDIS_PSW)

    /**
     * Obtains the address of the Redis database.
     * @return The Redis address, including host and port.
     */
    fun getRedisAddress() = getRequiredEnvVar(KEY_REDIS_ADDRESS)

    /**
     * Obtains the host name of the Redis database.
     * @return The Redis host.
     */
    fun getRedisHost() = getRedisAddress().split(":").first()

    /**
     * Obtains the port number of the Redis database.
     * @return The Redis port.
     */
    fun getRedisPort() = getRedisAddress().split(":").last().toInt()

    /**
     * Obtains the username to access the RabbitMQ broker.
     * @return The RabbitMQ username.
     */
    fun getRabbitUser() = getRequiredEnvVar(KEY_RABBIT_USR)

    /**
     * Obtains the password to access the RabbitMQ broker.
     * @return The RabbitMQ password
     */
    fun getRabbitPassword() = getRequiredEnvVar(KEY_RABBIT_PSW)

    /**
     * Obtains the address of the RabbitMQ broker.
     * @return The RabbitMQ address, including host and port.
     */
    fun getRabbitAddress() = getRequiredEnvVar(KEY_RABBIT_ADDRESS)

    /**
     * Obtains the host name of the RabbitMQ broker.
     * @return The RabbitMQ host.
     */
    fun getRabbitHost() = getRabbitAddress().split(":").first()

    /**
     * Obtains the port number of the RabbitMQ broker.
     * @return The RabbitMQ port.
     */
    fun getRabbitPort() = getRabbitAddress().split(":").last().toInt()

    /**
     * Obtains the hostname of the current machine.
     * @return The hostname of the machine.
     */
    fun getHostname() = getRequiredEnvVar(KEY_HOSTNAME)

    /**
     * Obtains the docker service name.
     * @return The service name.
     */
    fun getServiceName() = getRequiredEnvVar(KEY_SERVICE_NAME)
}
