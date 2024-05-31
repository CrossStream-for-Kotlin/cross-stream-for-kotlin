package pt.isel.leic.cs4k.independentBroker.network

import pt.isel.leic.cs4k.independentBroker.Neighbor
import java.nio.channels.AsynchronousSocketChannel

/**
 * Represents all information about inbound connection with a [Neighbor], i.e., the connection to receive messages.
 *
 * @property socketChannel The socket channel to receive messages.
 */
data class InboundConnection(
    val socketChannel: AsynchronousSocketChannel
)
