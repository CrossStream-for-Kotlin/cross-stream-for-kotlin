package pt.isel.leic.cs4k.independent.serviceDiscovery.config

/**
 * DNS Service Discovery configuration.
 *
 * @property serviceName The service name.
 */
data class DNSServiceDiscoveryConfig(
    val serviceName: String,
    override val periodicServiceDiscoveryUpdates: Long = DEFAULT_LOOKUP_AGAIN_TIME
) : ServiceDiscoveryConfig {

    private companion object {
        private const val DEFAULT_LOOKUP_AGAIN_TIME = 3000L
    }
}
