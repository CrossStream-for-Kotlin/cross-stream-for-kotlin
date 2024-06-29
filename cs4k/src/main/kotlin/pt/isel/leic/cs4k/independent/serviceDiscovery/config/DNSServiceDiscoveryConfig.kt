package pt.isel.leic.cs4k.independent.serviceDiscovery.config

/**
 * DNS Service Discovery configuration.
 *
 * @property serviceName The service name.
 */
data class DNSServiceDiscoveryConfig(
    val serviceName: String
) : ServiceDiscoveryConfig
