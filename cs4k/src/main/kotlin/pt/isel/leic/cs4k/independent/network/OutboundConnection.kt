package pt.isel.leic.cs4k.independent.network

import java.net.InetSocketAddress
import java.nio.channels.AsynchronousSocketChannel

/**
 * Represents all information about outbound connection with a [Neighbor], i.e., the connection to send messages.
 *
 * @property state The state of a connection.
 * @property inetSocketAddress The neighbor socket address.
 * @property socketChannel The socket channel to send messages.
 * @property numberOfConnectionAttempts The number of attempts to establish connections with the neighbor.
 */
data class OutboundConnection(
    val state: ConnectionState,
    val inetSocketAddress: InetSocketAddress,
    val socketChannel: AsynchronousSocketChannel? = null,
    val numberOfConnectionAttempts: Int = 0
) {

    val reachMaximumNumberOfConnectionAttempts
        get() = numberOfConnectionAttempts == DEFAULT_MAXIMUM_NUMBER_OF_CONNECTION_ATTEMPTS

    private companion object {
        private const val DEFAULT_MAXIMUM_NUMBER_OF_CONNECTION_ATTEMPTS = 3
    }
}
