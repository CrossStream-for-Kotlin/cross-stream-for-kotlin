package pt.isel.leic.cs4k.independent.serviceDiscovery

import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.common.BrokerException.UnexpectedBrokerException
import pt.isel.leic.cs4k.common.RetryExecutor
import pt.isel.leic.cs4k.independent.network.Neighbor
import pt.isel.leic.cs4k.independent.network.Neighbors
import java.lang.Thread.sleep
import java.net.Inet4Address
import java.net.InetAddress

/**
 * Responsible for service discovery through Domain Name System (DNS) queries, i.e.:
 *  - Periodic querying the DNS server, giving it a name.
 *  - Adding new entries to the neighbors.
 *
 * @property hostname The hostname.
 * @property serviceName The service name.
 * @property port The port number of neighbours.
 * @property neighbors The set of neighbors.
 * @property lookupAgainTime Amount of time, in milliseconds before another DNS query.
 */
class DNSServiceDiscovery(
    private val hostname: String,
    private val serviceName: String,
    private val port: Int,
    private val neighbors: Neighbors,
    private val lookupAgainTime: Long
) : ServiceDiscovery {

    // The node's own inet address (IP).
    private val selfInetAddress = InetAddress.getByName(hostname)

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Thread responsible for making periodic DNS queries.
    private val dnsLookupThread = Thread {
        retryExecutor.execute({ UnexpectedBrokerException() }, {
            try {
                while (true) {
                    logger.info("[{}:{}] querying dns server", selfInetAddress.hostAddress, port)
                    dnsLookup()
                    sleep(lookupAgainTime)
                }
            } catch (ex: Exception) {
                if (ex is InterruptedException) {
                    logger.error("[{}:{}] dns lookup interrupted", selfInetAddress.hostAddress, port)
                } else {
                    throw ex
                }
            }
        })
    }

    /**
     * Make a DNS query and add all new IP addresses received as neighbours.
     */
    private fun dnsLookup() {
        val neighborsIp = InetAddress.getAllByName(serviceName)
            .filter { it is Inet4Address && it != selfInetAddress }
        val currentNeighbors = neighborsIp
            .map { Neighbor(it, port) }
            .toSet()
        neighbors.addAll(currentNeighbors)
        logger.info("[{}:{}] dns lookup:: {}", selfInetAddress.hostAddress, port, neighborsIp.joinToString(" , "))
    }

    override fun start() {
        dnsLookupThread.start()
    }

    override fun stop() {
        dnsLookupThread.interrupt()
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(DNSServiceDiscovery::class.java)
    }
}
