package pt.isel.leic.cs4k.independent.serviceDiscovery

import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.common.BrokerException
import pt.isel.leic.cs4k.common.RetryExecutor
import pt.isel.leic.cs4k.independent.network.Neighbor
import pt.isel.leic.cs4k.independent.network.Neighbors
import pt.isel.leic.cs4k.independent.serviceDiscovery.utils.DatagramPacketInfo
import java.lang.Thread.sleep
import java.net.DatagramPacket
import java.net.Inet4Address
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
 * @property port The port to announce.
 * @param multicastIp The multicast ip.
 * @param multicastPort The multicast port.
 * @property sendDatagramPacketAgainTime Amount of time, in milliseconds before send another multicast datagram packet.
 * @param threadBuilder Creator of threads used for sending multicast packets and listening for neighbors..
 */
class MulticastServiceDiscovery(
    private val neighbors: Neighbors,
    private val selfIp: String,
    private val port: Int,
    multicastIp: String,
    multicastPort: Int,
    private val sendDatagramPacketAgainTime: Long,
    threadBuilder: Thread.Builder
) : ServiceDiscovery {

    // Multicast inet address (IP).
    private val multicastInetAddress = InetAddress.getByName(multicastIp)

    // Multicast socket address.
    private val multicastInetSocketAddress = InetSocketAddress(multicastInetAddress, multicastPort)

    // Buffer that stores the content of received multicast datagram packets.
    private val inboundBuffer = ByteArray(INBOUND_BUFFER_SIZE)

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Thread to listen for multicast datagram packet.
    private val listenMulticastSocketThread = threadBuilder.unstarted {
        retryExecutor.execute({ BrokerException.UnexpectedBrokerException() }, {
            val multicastSocket = MulticastSocket(multicastPort)
            val networkInterface = getActiveMulticastNetworkInterface()
            joinMulticastGroup(multicastSocket, networkInterface)
            listenMulticastSocket(multicastSocket, networkInterface)
        })
    }

    // Thread to periodic announce existence to neighbors.
    private val periodicAnnounceExistenceToNeighborsThread = threadBuilder.unstarted {
        retryExecutor.execute({ BrokerException.UnexpectedBrokerException() }, {
            val port = getPort()
            val multicastSocket = MulticastSocket(port)
            periodicAnnounceExistenceToNeighbors(multicastSocket)
        })
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
        logger.info("[{}:{}] reading multicast socket", selfIp, port)
        while (!listenMulticastSocketThread.isInterrupted) {
            try {
                val receivedDatagramPacket = DatagramPacket(inboundBuffer, inboundBuffer.size)
                multicastSocket.receive(receivedDatagramPacket)

                processDatagramPacket(receivedDatagramPacket)
            } catch (ex: Exception) {
                multicastSocket.leaveGroup(multicastInetSocketAddress, networkInterface)
                multicastSocket.close()
                if (ex is InterruptedException) {
                    logger.info("[{}:{}] stop reading multicast socket", selfIp, port)
                    break
                } else {
                    throw ex
                }
            }
        }
    }

    /**
     * Process received datagrams packets.
     *
     * @param datagramPacket The datagram packet received.
     */
    private fun processDatagramPacket(datagramPacket: DatagramPacket) {
        val neighbourPort = DatagramPacketInfo
            .deserialize(String(datagramPacket.data.copyOfRange(0, datagramPacket.length)))
            .port
        val neighborInetAddress = datagramPacket.address

        if (selfIp != LOOP_BACK_IP && neighborInetAddress.hostAddress != selfIp) {
            neighbors.add(Neighbor(neighborInetAddress, neighbourPort))
            logger.info("[{}:{}] <++ '{}':'{}'", selfIp, port, neighborInetAddress, neighbourPort)
        } else if (selfIp == LOOP_BACK_IP && neighbourPort != port) {
            val neighborLoopBackInetAddress = InetAddress.getByName(LOOP_BACK_IP)
            neighbors.add(Neighbor(neighborLoopBackInetAddress, neighbourPort))
            logger.info("[{}:{}] <++ '{}':'{}'", selfIp, port, neighborLoopBackInetAddress, neighbourPort)
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
                val messageBytes = DatagramPacketInfo.serialize(DatagramPacketInfo(port)).toByteArray()
                val datagramPacket = DatagramPacket(messageBytes, messageBytes.size, multicastInetSocketAddress)

                multicastSocket.send(datagramPacket)
                logger.info("[{}:{}] ++> '{}':'{}'", selfIp, port, selfIp, port)
                sleep(sendDatagramPacketAgainTime)
            } catch (ex: Exception) {
                multicastSocket.close()
                if (ex is InterruptedException) {
                    logger.info("[{}:{}] stop announce", selfIp, port)
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
            if (networkInterface.isUp && networkInterface.supportsMulticast() && !networkInterface.isLoopback) {
                if (selfIp != LOOP_BACK_IP) {
                    return networkInterface
                } else if (!networkInterface.name.startsWith("eth") &&
                    networkInterface.inetAddresses().anyMatch { it is Inet4Address }
                ) {
                    return networkInterface
                }
            }
        }
        throw Exception("[$selfIp] There is no active network interface that supports multicast!")
    }

    override fun start() {
        listenMulticastSocketThread.start()
        periodicAnnounceExistenceToNeighborsThread.start()
    }

    override fun stop() {
        listenMulticastSocketThread.interrupt()
        periodicAnnounceExistenceToNeighborsThread.interrupt()
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(MulticastServiceDiscovery::class.java)

        private const val INBOUND_BUFFER_SIZE = 1024
        private const val TIME_TO_LIVE = 10
        private const val LOOP_BACK_IP = "127.0.0.1"
        private const val FIRST_AVAILABLE_PORT = 6700
        private const val LAST_AVAILABLE_PORT = 6900

        /**
         * Get a port number to announce existence to neighbours.
         */
        private fun getPort() = (FIRST_AVAILABLE_PORT..LAST_AVAILABLE_PORT).random()
    }
}
