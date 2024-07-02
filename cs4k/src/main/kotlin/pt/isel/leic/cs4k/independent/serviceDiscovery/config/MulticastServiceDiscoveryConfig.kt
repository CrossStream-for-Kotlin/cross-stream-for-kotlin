package pt.isel.leic.cs4k.independent.serviceDiscovery.config

/**
 * Multicast Service Discovery configuration.
 *
 * @param multicastIp The multicast ip.
 * @param multicastPort The multicast port.
 */
data class MulticastServiceDiscoveryConfig(
    val multicastIp: String = DEFAULT_MULTICAST_IP,
    val multicastPort: Int = DEFAULT_MULTICAST_PORT,
    override val periodicServiceDiscoveryUpdates: Long = DEFAULT_SEND_DATAGRAM_PACKET_AGAIN_TIME
) : ServiceDiscoveryConfig {

    init {
        if (!isMulticastIpValid(multicastIp)) {
            throw IllegalArgumentException(
                "The multicast ip must be between '224.0.0.1' and '239.255.255.255'."
            )
        }
        if (!isMulticastPortValid(multicastPort)) {
            throw IllegalArgumentException("The multicast port must be between $FIRST_AVAILABLE_PORT and $LAST_AVAILABLE_PORT.")
        }
    }

    private companion object {
        private const val DEFAULT_SEND_DATAGRAM_PACKET_AGAIN_TIME = 3000L
        private const val DEFAULT_MULTICAST_IP = "228.5.6.7"
        private const val DEFAULT_MULTICAST_PORT = 6790
        private const val FIRST_AVAILABLE_PORT = 6700
        private const val LAST_AVAILABLE_PORT = 6900

        /**
         * Check if multicast IP is valid.
         * First available multicast ip is '224.0.0.1' and the last one is '239.255.255.255'.
         *
         * @param multicastIp The multicast ip to check.
         * @return True if multicast ip is valid.
         * @see [MulticastSocket](https://docs.oracle.com/javase%2F8%2Fdocs%2Fapi%2F%2F/java/net/MulticastSocket.html)
         */
        private fun isMulticastIpValid(multicastIp: String): Boolean {
            val regex = "^([0-9]{1,3}\\.){3}[0-9]{1,3}$"

            if (!multicastIp.matches(regex.toRegex())) {
                return false
            }

            val parts = multicastIp.split(".")
            if (parts.size != 4) {
                return false
            }

            val octets = parts.map { it.toIntOrNull() ?: return false }

            val firstOctet = octets[0]
            val secondOctet = octets[1]
            val thirdOctet = octets[2]
            val fourthOctet = octets[3]

            if (firstOctet !in 224..239) {
                return false
            }

            if (firstOctet == 224 && (secondOctet != 0 || thirdOctet != 0 || fourthOctet == 0)) {
                return false
            }

            if (firstOctet == 239 && secondOctet == 255 && thirdOctet == 255 && fourthOctet == 255) {
                return true
            }

            return true
        }

        /**
         * Check if multicast port is valid.
         *
         * @param multicastPort The multicast port to check.
         * @return True if multicast port is valid.
         */
        private fun isMulticastPortValid(multicastPort: Int): Boolean =
            multicastPort in FIRST_AVAILABLE_PORT..LAST_AVAILABLE_PORT
    }
}
