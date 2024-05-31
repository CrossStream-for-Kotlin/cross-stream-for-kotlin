package pt.isel.leic.cs4k.independentBroker.network

import pt.isel.leic.cs4k.independentBroker.Neighbor
import java.net.InetAddress
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Responsible for storing information about neighbors on the network.
 */
class Neighbors {

    // The hash set of neighbors.
    private val set = hashSetOf<Neighbor>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Get all neighbors.
     *
     * @return The set of neighbors.
     */
    fun getAll() = lock.withLock {
        set.toSet()
    }

    /**
     * Add a neighbor if it doesn't exist yet.
     *
     * @param neighbor The neighbor to add.
     */
    fun add(neighbor: Neighbor) {
        lock.withLock {
            if (set.none { it.inetAddress == neighbor.inetAddress }) {
                set.add(neighbor)
            }
        }
    }

    /**
     * Add new neighbors if they don't exist yet.
     *
     * @param neighbors The neighbors to add.
     */
    fun addAll(neighbors: Set<Neighbor>) {
        lock.withLock {
            set.addAll(neighbors.filterNot { neighbor -> set.any { it.inetAddress == neighbor.inetAddress } })
        }
    }

    /**
     * Remove a neighbor if it exists.
     *
     * @param neighbor The neighbor to remove.
     */
    fun remove(neighbor: Neighbor) {
        lock.withLock {
            set.removeIf { it.inetAddress == neighbor.inetAddress }
        }
    }

    /**
     * Updates a neighbor's inbound connection. If it doesn't exist, it is still added.
     *
     * @param inetAddress The inet address (IP) of the neighbor.
     * @param inboundConnection The updated inbound connection.
     * @return The updated neighbour.
     */
    fun updateAndGet(inetAddress: InetAddress, inboundConnection: InboundConnection): Neighbor =
        lock.withLock {
            val neighbor = set.find { it.inetAddress == inetAddress }
            return@withLock if (neighbor != null) {
                remove(neighbor)
                val updatedNeighbor = neighbor.copy(inboundConnection = inboundConnection)
                add(updatedNeighbor)
                updatedNeighbor
            } else {
                val newNeighbor = Neighbor(
                    inetAddress = inetAddress,
                    inboundConnection = inboundConnection
                )
                add(newNeighbor)
                newNeighbor
            }
        }

    /**
     * Update a neighbor. If it doesn't exist, it is still added.
     *
     * @param neighbor The updated neighbour.
     */
    fun update(neighbor: Neighbor) =
        lock.withLock {
            remove(neighbor)
            add(neighbor)
        }
}
