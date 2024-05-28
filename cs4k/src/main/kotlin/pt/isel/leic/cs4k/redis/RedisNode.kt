package pt.isel.leic.cs4k.redis

/**
 * Represents a Redis node.
 *
 * @property host The host name.
 * @property port The port number.
 */
data class RedisNode(
    val host: String,
    val port: Int
)
