package pt.isel.leic.cs4k.independent.network

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
     * Get a neighbor.
     *
     * @param inetAddress The neighbour inetAddress.
     * @param port The neighbour port number.
     * @return The neighbor if exists.
     */
    fun get(inetAddress: InetAddress, port: Int) = lock.withLock {
        set.find { it.inetAddress == inetAddress && it.port == port }
    }

    /**
     * Add a neighbor if it doesn't exist yet.
     *
     * @param neighbor The neighbor to add.
     */
    fun add(neighbor: Neighbor) {
        lock.withLock {
            if (set.none { it.inetAddress == neighbor.inetAddress && it.port == neighbor.port }) {
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
            set.removeIf { it.inetAddress == neighbor.inetAddress && it.port == neighbor.port }
        }
    }

    /**
     * Updates a neighbor's inbound connection, if it exists.
     *
     * @param inetAddress The inet address (IP) of the neighbor.
     * @param inboundConnection The updated inbound connection.
     */
    fun updateInboundConnection(inetAddress: InetAddress, inboundConnection: InboundConnection?) {
        lock.withLock {
            val neighbor = set.find { it.inetAddress.hostAddress != LOOP_BACK_IP && it.inetAddress == inetAddress }
            if (neighbor != null) {
                remove(neighbor)
                val updatedNeighbor = neighbor.copy(inboundConnection = inboundConnection)
                add(updatedNeighbor)
            }
        }
    }

    /**
     * Update a neighbor, if it exists.
     *
     * @param neighbor The updated neighbour.
     */
    fun update(neighbor: Neighbor) {
        lock.withLock {
            if (noLongerNeighbor(neighbor)) return@withLock
            remove(neighbor)
            add(neighbor)
        }
    }

    /**
     * Check if the neighbor is no longer a neighbor.
     *
     * @param neighbor The neighbour to check.
     * @return True if the neighbor is no longer a neighbor.
     */
    private fun noLongerNeighbor(neighbor: Neighbor) = lock.withLock {
        set.none { it.inetAddress == neighbor.inetAddress && it.port == neighbor.port }
    }

    companion object {
        const val LOOP_BACK_IP = "127.0.0.1"
    }
}
