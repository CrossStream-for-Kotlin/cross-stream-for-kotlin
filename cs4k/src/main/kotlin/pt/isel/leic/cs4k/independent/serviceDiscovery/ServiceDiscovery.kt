package pt.isel.leic.cs4k.independent.serviceDiscovery

/**
 * Public contract of Service Discovery.
 */
interface ServiceDiscovery {

    /**
     * Start service discovery.
     */
    fun start()

    /**
     * Stop service discovery.
     */
    fun stop()
}
