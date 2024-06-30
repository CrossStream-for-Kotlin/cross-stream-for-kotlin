package pt.isel.leic.cs4k.independent

import org.junit.jupiter.api.BeforeEach
import pt.isel.leic.cs4k.independent.serviceDiscovery.config.MulticastServiceDiscoveryConfig
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BrokerIndependentTests {

    @BeforeEach
    fun setUp() {
        Thread.sleep(5000)
    }

    @Test
    fun `1 subscriber in 1 topic waiting for 1 message`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        val latch = CountDownLatch(1)

        val unsubscribe = brokerInstances.first().subscribe(
            topic = topic,
            handler = { event ->
                // Assert [1]
                assertEquals(topic, event.topic)
                assertEquals(IGNORE_EVENT_ID, event.id)
                assertEquals(message, event.message)
                assertFalse(event.isLast)
                latch.countDown()
            }
        )

        // Act
        brokerInstances.first().publish(
            topic = topic,
            message = message
        )

        // Assert [2]
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        // Clean Up
        unsubscribe()
    }

    @Test
    fun `n subscribers in 1 topic waiting for 1 message with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS)
        val unsubscribes = mutableListOf<() -> Unit>()

        repeat(NUMBER_OF_SUBSCRIBERS) {
            val unsubscribe = getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    // Assert [1]
                    assertEquals(topic, event.topic)
                    assertEquals(IGNORE_EVENT_ID, event.id)
                    assertEquals(message, event.message)
                    assertFalse(event.isLast)
                    latch.countDown()
                }
            )
            unsubscribes.add(unsubscribe)
        }

        // Act
        brokerInstances.first().publish(
            topic = topic,
            message = message
        )

        // Assert [2]
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    private companion object {

        private const val IGNORE_EVENT_ID = -1L
        private const val NUMBER_OF_BROKER_INSTANCES = 2
        private const val NUMBER_OF_SUBSCRIBERS = 10

        private const val SUBSCRIBE_TIMEOUT_MILLIS = 120000L

        val brokerInstances = List(NUMBER_OF_BROKER_INSTANCES) {
            BrokerIndependent("127.0.0.1", MulticastServiceDiscoveryConfig)
        }

        private fun getRandomBrokerInstance() = brokerInstances.random()
        private fun generateRandom() = abs(Random.nextLong())
        private fun newRandomTopic() = "topic${generateRandom()}"
        private fun newRandomMessage() = "message${generateRandom()}"
    }
}
