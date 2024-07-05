package pt.isel.leic.cs4k.independent.serviceDiscovery.config

/**
 * Public contract of Service Discovery configuration.
 *
 * @property periodicServiceDiscoveryUpdates Time between periodic service discovery updates.
 */
interface ServiceDiscoveryConfig {
    val periodicServiceDiscoveryUpdates: Long
}
