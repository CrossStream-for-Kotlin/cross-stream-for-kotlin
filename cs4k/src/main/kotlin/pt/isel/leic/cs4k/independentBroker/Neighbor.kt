package cs4k.prototype.broker.option3

import pt.isel.leic.cs4k.independentBroker.network.ConnectionState
import pt.isel.leic.cs4k.independentBroker.network.InboundConnection
import pt.isel.leic.cs4k.independentBroker.network.OutboundConnection
import java.net.InetAddress

/**
 * Represents a neighbor on the network.
 *
 * @property inetAddress The inet address (IP).
 * @property inboundConnection All information about inbound connection, i.e., the connection to receive events.
 * @property outboundConnection All information about outbound connection, i.e, the connection to send events.
 */
data class Neighbor(
    val inetAddress: InetAddress,
    val inboundConnection: InboundConnection? = null,
    val outboundConnection: OutboundConnection? = null
) {

    val isOutboundConnectionActive
        get() = outboundConnection?.state == ConnectionState.CONNECTED

    val isInboundConnectionActive
        get() = inboundConnection != null
}
