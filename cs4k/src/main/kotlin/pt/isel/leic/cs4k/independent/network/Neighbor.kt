package pt.isel.leic.cs4k.independent.network

import pt.isel.leic.cs4k.common.Event
import pt.isel.leic.cs4k.independent.messaging.MessageQueue
import java.net.InetAddress

/**
 * Represents a neighbor on the network.
 *
 * @property inetAddress The inet address (IP).
 * @property port The port number.
 * @property inboundConnection All information about inbound connection, i.e., the connection to receive events.
 * @property outboundConnection All information about outbound connection, i.e, the connection to send events.
 * @property eventQueue The list of events to be sent to neighbour.
 */
data class Neighbor(
    val inetAddress: InetAddress,
    val port: Int,
    val inboundConnection: InboundConnection? = null,
    val outboundConnection: OutboundConnection? = null,
    val eventQueue: MessageQueue<Event> = MessageQueue(EVENTS_TO_PROCESS_CAPACITY)
) {

    val isOutboundConnectionActive
        get() = outboundConnection?.state == ConnectionState.CONNECTED

    private companion object {
        private const val EVENTS_TO_PROCESS_CAPACITY = 1_000_000
    }
}
