package pt.isel.leic.cs4k.independent.config

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import pt.isel.leic.cs4k.independent.serviceDiscovery.config.MulticastServiceDiscoveryConfig

class MulticastServiceDiscoveryConfigTests {

    @Test
    fun `not thrown an exception with valid multicast ip`() {
        // Arrange
        // Act
        // Assert
        assertDoesNotThrow {
            MulticastServiceDiscoveryConfig(multicastIp = "224.0.0.1")
            MulticastServiceDiscoveryConfig(multicastIp = "225.4.5.6")
            MulticastServiceDiscoveryConfig(multicastIp = "239.255.255.255")
        }
    }

    @Test
    fun `thrown an exception with invalid multicast ip`() {
        // Arrange
        // Act
        // Assert
        assertThrows(IllegalArgumentException::class.java) {
            MulticastServiceDiscoveryConfig(multicastIp = "224.0.0.0")
            MulticastServiceDiscoveryConfig(multicastIp = "240.255.255.255")
        }
    }

    @Test
    fun `not thrown an exception with valid multicast port`() {
        // Arrange
        // Act
        // Assert
        assertDoesNotThrow {
            MulticastServiceDiscoveryConfig(multicastPort = 6700)
            MulticastServiceDiscoveryConfig(multicastPort = 6750)
            MulticastServiceDiscoveryConfig(multicastPort = 6900)
        }
    }

    @Test
    fun `thrown an exception with invalid multicast port`() {
        // Arrange
        // Act
        // Assert
        assertThrows(IllegalArgumentException::class.java) {
            MulticastServiceDiscoveryConfig(multicastPort = 6699)
            MulticastServiceDiscoveryConfig(multicastPort = 6901)
        }
    }
}
