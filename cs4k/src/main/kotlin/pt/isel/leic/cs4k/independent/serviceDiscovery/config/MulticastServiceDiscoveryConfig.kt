package pt.isel.leic.cs4k.independent.serviceDiscovery.config

/**
 * Multicast Service Discovery configuration.
 *
 * @property hostname The hostname.
 */
data class MulticastServiceDiscoveryConfig(
    val hostname: String
) : ServiceDiscoveryConfig
