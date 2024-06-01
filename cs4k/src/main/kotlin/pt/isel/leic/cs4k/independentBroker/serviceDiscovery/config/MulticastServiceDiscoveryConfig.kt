package pt.isel.leic.cs4k.independentBroker.serviceDiscovery.config

/**
 * Multicast Service Discovery configuration.
 *
 * @property hostname The hostname.
 */
data class MulticastServiceDiscoveryConfig(
    val hostname: String
) : ServiceDiscoveryConfig
