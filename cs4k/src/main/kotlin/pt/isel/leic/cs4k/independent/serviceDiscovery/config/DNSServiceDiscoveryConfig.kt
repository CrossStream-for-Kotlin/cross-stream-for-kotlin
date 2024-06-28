package pt.isel.leic.cs4k.independent.serviceDiscovery.config

/**
 * DNS Service Discovery configuration.
 *
 * @property hostname The hostname.
 * @property serviceName The service name.
 */
data class DNSServiceDiscoveryConfig(
    val hostname: String,
    val serviceName: String
) : ServiceDiscoveryConfig
