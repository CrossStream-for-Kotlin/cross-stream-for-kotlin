package pt.isel.leic.cs4k.rabbitmq

/**
 * Represents a Rabbit node.
 *
 * @property host The host name.
 * @property port The port number.
 */
data class RabbitNode(
    val host: String,
    val port: Int
)
