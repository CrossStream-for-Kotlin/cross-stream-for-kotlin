package pt.isel.leic.cs4k.independentBroker.serviceDiscovery.config

/**
 * DNS Service Discovery configuration.
 *
 * @property hostname The hostname.
 * @property serviceName The Docker service name.
 */
data class DNSServiceDiscoveryConfig(
    val hostname: String,
    val serviceName: String
) : ServiceDiscoveryConfig
