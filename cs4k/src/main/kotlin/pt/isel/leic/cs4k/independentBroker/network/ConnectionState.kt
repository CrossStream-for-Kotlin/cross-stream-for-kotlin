package pt.isel.leic.cs4k.independentBroker.network

import pt.isel.leic.cs4k.independentBroker.Neighbor

/**
 * Represents the state of a connection with a [Neighbor].
 *
 * - 'CONNECTED': Active connection with neighbor.
 * - 'DISCONNECTED': Disconnect connection with neighbor.
 * - 'ZOMBIE': Something is wrong with the connection with the neighbor, but the neighbor exists.
 */
enum class ConnectionState {
    CONNECTED, DISCONNECTED, ZOMBIE
}
