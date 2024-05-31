package pt.isel.leic.cs4k.independentBroker.serviceDiscovery

import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.common.BrokerException
import pt.isel.leic.cs4k.common.RetryExecutor
import pt.isel.leic.cs4k.independentBroker.Neighbor
import pt.isel.leic.cs4k.independentBroker.network.Neighbors
import java.lang.Thread.sleep
import java.net.DatagramPacket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.MulticastSocket
import java.net.NetworkInterface

/**
 * Responsible for service discovery through multicast, i.e.:
 *  - Periodic announce existence to neighbors via a multicast datagram packet.
 *  - Process all receive multicast datagram packet to discover neighbors.
 *
 * @property neighbors The set of neighbors.
 * @property selfIp The node's own IP address.
 * @property sendDatagramPacketAgainTime Amount of time, in milliseconds before send another multicast datagram packet.
 */
class MulticastServiceDiscovery(
    private val neighbors: Neighbors,
    private val selfIp: String,
    private val sendDatagramPacketAgainTime: Long = DEFAULT_SEND_DATAGRAM_PACKET_AGAIN_TIME
) {

    // Multicast inet address (IP).
    private val multicastInetAddress = InetAddress.getByName(MULTICAST_IP)

    // Multicast socket address.
    private val multicastInetSocketAddress = InetSocketAddress(multicastInetAddress, MULTICAST_PORT)

    // Buffer that stores the content of received multicast datagram packets.
    private val inboundBuffer = ByteArray(INBOUND_BUFFER_SIZE)

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Thread to listen for multicast datagram packet.
    private val listenMulticastSocketThread = Thread {
        retryExecutor.execute({ BrokerException.UnexpectedBrokerException() }, {
            val multicastSocket = MulticastSocket(MULTICAST_PORT)
            val networkInterface = getActiveMulticastNetworkInterface()
            joinMulticastGroup(multicastSocket, networkInterface)
            listenMulticastSocket(multicastSocket, networkInterface)
        })
    }

    // Thread to periodic announce existence to neighbors.
    private val periodicAnnounceExistenceToNeighborsThread = Thread {
        retryExecutor.execute({ BrokerException.UnexpectedBrokerException() }, {
            val multicastSocket = MulticastSocket(MULTICAST_PORT)
            periodicAnnounceExistenceToNeighbors(multicastSocket)
        })
    }

    init {
        listenMulticastSocketThread.start()
        periodicAnnounceExistenceToNeighborsThread.start()
    }

    /**
     * Join the multicast group.
     *
     * @param multicastSocket The multicast socket to join to.
     * @param networkInterface The network interface that supports multicast.
     */
    private fun joinMulticastGroup(multicastSocket: MulticastSocket, networkInterface: NetworkInterface) {
        // Redefine the Time To Live value of IP multicast packets sent.
        // I.e. The maximum number of machine-to-machine hops that packets can make before being discarded.
        multicastSocket.timeToLive = TIME_TO_LIVE

        multicastSocket.joinGroup(multicastInetSocketAddress, networkInterface)
    }

    /**
     * Blocks the thread reading the socket and processes multicast datagram packet received.
     *
     * @param multicastSocket The multicast socket to listen to.
     * @param networkInterface The network interface that supports multicast.
     */
    private fun listenMulticastSocket(multicastSocket: MulticastSocket, networkInterface: NetworkInterface) {
        logger.info("[{}] reading multicast socket", selfIp)
        while (!listenMulticastSocketThread.isInterrupted) {
            try {
                val receivedDatagramPacket = DatagramPacket(inboundBuffer, inboundBuffer.size)
                multicastSocket.receive(receivedDatagramPacket)
                val remoteInetAddress = receivedDatagramPacket.address
                val message = String(receivedDatagramPacket.data, 0, receivedDatagramPacket.length)
                val parts = message.split(":")
                val remoteIp = parts[0]
                if (remoteInetAddress.hostAddress != selfIp) {
                    neighbors.add(
                        Neighbor(
                            InetAddress.getByName(remoteIp)
                        )
                    )
                    logger.info("[{}] <++ '{}'", selfIp, remoteIp)
                } else {
                    logger.info("[{}] ignoring packet from self", selfIp)
                }
            } catch (ex: Exception) {
                multicastSocket.leaveGroup(multicastInetSocketAddress, networkInterface)
                multicastSocket.close()
                if (ex is InterruptedException) {
                    logger.info("[{}] stop reading multicast socket", selfIp)
                    break
                } else {
                    throw ex
                }
            }
        }
    }

    /**
     * Periodic announce the existence to neighbors by sending a multicast datagram packet.
     *
     * @param multicastSocket The multicast socket to send to.
     */
    private fun periodicAnnounceExistenceToNeighbors(multicastSocket: MulticastSocket) {
        while (!periodicAnnounceExistenceToNeighborsThread.isInterrupted) {
            try {
                val messageBytes = "$selfIp".toByteArray()
                val datagramPacket = DatagramPacket(messageBytes, messageBytes.size, multicastInetSocketAddress)
                multicastSocket.send(datagramPacket)
                logger.info("[{}] ++> '{}'", selfIp, selfIp)
                sleep(sendDatagramPacketAgainTime)
            } catch (ex: Exception) {
                multicastSocket.close()
                if (ex is InterruptedException) {
                    logger.info("[{}] stop announce", selfIp)
                    break
                } else {
                    throw ex
                }
            }
        }
    }

    /**
     * Get one active network interface that supports multicast.
     *
     * @return The first network interface find that supports multicast.
     * @throws Exception If there is no active network interface that supports multicast
     */
    private fun getActiveMulticastNetworkInterface(): NetworkInterface {
        val networkInterfaces = NetworkInterface.getNetworkInterfaces()
        while (networkInterfaces.hasMoreElements()) {
            val networkInterface = networkInterfaces.nextElement()
            if (networkInterface.isUp && networkInterface.supportsMulticast()) {
                return networkInterface
            }
        }
        throw Exception("[$selfIp] There is no active network interface that supports multicast!")
    }

    /**
     * Stop service discovery.
     */
    fun stop() {
        listenMulticastSocketThread.interrupt()
        periodicAnnounceExistenceToNeighborsThread.interrupt()
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(MulticastServiceDiscovery::class.java)

        private const val MULTICAST_IP = "228.5.6.7"
        private const val MULTICAST_PORT = 6789
        private const val INBOUND_BUFFER_SIZE = 1024
        private const val TIME_TO_LIVE = 10
        private const val DEFAULT_SEND_DATAGRAM_PACKET_AGAIN_TIME = 60_000L
    }
}
