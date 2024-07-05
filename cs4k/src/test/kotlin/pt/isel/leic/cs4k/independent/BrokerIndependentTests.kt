package pt.isel.leic.cs4k.independent

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import pt.isel.leic.cs4k.common.BrokerException.BrokerTurnOffException
import pt.isel.leic.cs4k.common.Event
import pt.isel.leic.cs4k.independent.serviceDiscovery.config.MulticastServiceDiscoveryConfig
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

class BrokerIndependentTests {

    @BeforeEach
    fun setUp() {
        Thread.sleep(2_000)
    }

    // ---- simple tests ----

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

        val latch = CountDownLatch(1)
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

    @Test
    fun `1 subscriber in 1 topic receiving n messages with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = List(NUMBER_OF_MESSAGES) { newRandomMessage() }

        val latch = CountDownLatch(NUMBER_OF_MESSAGES)
        val eventsReceived = ConcurrentLinkedQueue<Event>()

        val unsubscribe = brokerInstances.first().subscribe(
            topic = topic,
            handler = { event ->
                eventsReceived.add(event)
                latch.countDown()
            }
        )

        // Act
        messages.forEach { msg ->
            getRandomBrokerInstance().publish(
                topic = topic,
                message = msg
            )
        }

        // Assert
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        assertEquals(NUMBER_OF_MESSAGES, eventsReceived.size)
        eventsReceived.forEach { event ->
            assertEquals(topic, event.topic)
            assertEquals(IGNORE_EVENT_ID, event.id)
            assertContains(messages, event.message)
            assertFalse(event.isLast)
        }

        // Clean Up
        unsubscribe()
    }

    @Test
    fun `n subscribers in 1 topic receiving n messages with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = List(NUMBER_OF_MESSAGES) { newRandomMessage() }

        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)
        val unsubscribes = mutableListOf<() -> Unit>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()

        repeat(NUMBER_OF_SUBSCRIBERS) {
            val unsubscribe = getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    eventsReceived.add(event)
                    latch.countDown()
                }
            )
            unsubscribes.add(unsubscribe)
        }

        // Act
        messages.forEach { message ->
            getRandomBrokerInstance().publish(
                topic = topic,
                message = message
            )
        }

        // Assert
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

        val eventsReceivedSet = eventsReceived.toSet()
        assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)
        eventsReceivedSet.forEach { event ->
            assertEquals(topic, event.topic)
            assertEquals(IGNORE_EVENT_ID, event.id)
            assertContains(messages, event.message)
            assertFalse(event.isLast)
        }

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    @Test
    fun `n subscribers in n topics receiving n messages with several broker instances involved`() {
        // Arrange
        val topicsAndMessages = (1..NUMBER_OF_TOPICS).associate {
            newRandomTopic() to List(NUMBER_OF_MESSAGES) { newRandomMessage() }
        }

        val latch = CountDownLatch(NUMBER_OF_TOPICS * NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)
        val unsubscribes = mutableListOf<() -> Unit>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()

        // Act
        topicsAndMessages.forEach { entry ->
            repeat(NUMBER_OF_SUBSCRIBERS) {
                val unsubscribe = getRandomBrokerInstance().subscribe(
                    topic = entry.key,
                    handler = { event ->
                        eventsReceived.add(event)
                        latch.countDown()
                    }
                )
                unsubscribes.add(unsubscribe)
            }
        }

        topicsAndMessages.forEach { entry ->
            entry.value.forEach { message ->
                getRandomBrokerInstance().publish(
                    topic = entry.key,
                    message = message
                )
            }
        }

        // Assert
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        assertEquals(NUMBER_OF_TOPICS * NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

        eventsReceived.forEach { event ->
            val entry = topicsAndMessages[event.topic]
            assertNotNull(entry)
            assertContains(entry, event.message)
            assertFalse(event.isLast)
        }

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    @Test
    fun `subscriber unsubscribing should not receive message`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        val latch = CountDownLatch(1)

        val unsubscribe = brokerInstances.first().subscribe(topic) { _ ->
            // Assert
            fail("Event was emitted, however it should have unsubscribed.")
        }

        // Act
        unsubscribe()

        brokerInstances.first().publish(
            topic = topic,
            message = message
        )

        thread {
            Thread.sleep(4000)
            latch.countDown()
        }

        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)
    }

    @Test
    fun `subscribers unsubscribing should not receive message with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        val latch = CountDownLatch(1)

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val unsubscribe = getRandomBrokerInstance().subscribe(topic) { _ ->
                // Assert
                fail("Event was emitted, however it should have unsubscribed.")
            }
            unsubscribe()
        }

        brokerInstances.first().publish(
            topic = topic,
            message = message
        )

        thread {
            Thread.sleep(4000)
            latch.countDown()
        }

        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)
    }

    // ---- stress tests ----

    @Test
    fun `stress test with simultaneous publication of n messages to 1 topic with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = List(NUMBER_OF_MESSAGES) { newRandomMessage() }

        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)
        val unsubscribes = mutableListOf<() -> Unit>()
        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()

        repeat(NUMBER_OF_SUBSCRIBERS) {
            val unsubscribe = getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    eventsReceived.add(event)
                    latch.countDown()
                }
            )
            unsubscribes.add(unsubscribe)
        }

        // Act
        messages.forEach { message ->
            val th = Thread {
                try {
                    getRandomBrokerInstance().publish(
                        topic = topic,
                        message = message
                    )
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start().also { threads.add(th) }
        }
        threads.forEach { it.join() }

        // Assert
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

        val eventsReceivedSet = eventsReceived
            .toSet()
            .map { event -> event.message }
        assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)
        assertTrue(eventsReceivedSet.containsAll(messages))

        if (errors.isNotEmpty()) throw errors.peek()

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    @Test
    fun `stress test with simultaneous publication of n messages to n topics with several broker instances involved`() {
        // Arrange
        val topicsAndMessages = (1..NUMBER_OF_TOPICS).associate {
            newRandomTopic() to List(NUMBER_OF_MESSAGES) { newRandomMessage() }
        }

        val threads = ConcurrentLinkedQueue<Thread>()
        val unsubscribes = mutableListOf<() -> Unit>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS)

        topicsAndMessages.forEach { entry ->
            repeat(NUMBER_OF_SUBSCRIBERS) {
                val unsubscribe = getRandomBrokerInstance().subscribe(
                    topic = entry.key,
                    handler = { event ->
                        eventsReceived.add(event)
                        latch.countDown()
                    }
                )
                unsubscribes.add(unsubscribe)
            }
        }

        // Act
        topicsAndMessages.forEach { entry ->
            entry.value.forEach { message ->
                val th = Thread {
                    try {
                        getRandomBrokerInstance().publish(
                            topic = entry.key,
                            message = message
                        )
                    } catch (e: Exception) {
                        errors.add(e)
                    }
                }
                th.start().also { threads.add(th) }
            }
        }
        threads.forEach { it.join() }

        // Assert
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS, eventsReceived.size)
        topicsAndMessages.forEach { entry ->
            val eventsReceivedSet = eventsReceived
                .filter { it.topic == entry.key }
                .toSet()
                .map { event -> event.message }
            assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)

            val messages = topicsAndMessages[entry.key]
            assertNotNull(messages)
            assertTrue(eventsReceivedSet.containsAll(messages))
        }

        if (errors.isNotEmpty()) throw errors.peek()

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    // ---- shutdown tests ----

    @Test
    fun `cannot invoke method shutdown twice`() {
        // Arrange
        val broker = BrokerIndependent("127.0.0.1", MulticastServiceDiscoveryConfig())

        Thread.sleep(5000)

        broker.shutdown()

        // Assert
        assertFailsWith<BrokerTurnOffException> {
            // Act
            broker.shutdown()
        }
    }

    @Test
    fun `cannot invoke method subscribe after shutdown`() {
        // Arrange
        val broker = BrokerIndependent("127.0.0.1", MulticastServiceDiscoveryConfig())

        Thread.sleep(5000)

        broker.shutdown()

        // Assert
        assertFailsWith<BrokerTurnOffException> {
            // Act
            broker.subscribe(newRandomTopic()) { _ -> }
        }
    }

    @Test
    fun `cannot invoke method publish after shutdown`() {
        // Arrange
        val broker = BrokerIndependent("127.0.0.1", MulticastServiceDiscoveryConfig())

        Thread.sleep(5000)

        broker.shutdown()

        // Assert
        assertFailsWith<BrokerTurnOffException> {
            // Act
            broker.publish(newRandomTopic(), newRandomMessage())
        }
    }

    private companion object {

        private const val IGNORE_EVENT_ID = -1L
        private const val NUMBER_OF_BROKER_INSTANCES = 5
        private const val NUMBER_OF_TOPICS = 5
        private const val NUMBER_OF_SUBSCRIBERS = 200
        private const val NUMBER_OF_MESSAGES = 200

        private const val SUBSCRIBE_TIMEOUT_MILLIS = 120000L

        val brokerInstances = List(NUMBER_OF_BROKER_INSTANCES) {
            BrokerIndependent("127.0.0.1", MulticastServiceDiscoveryConfig())
        }

        private fun getRandomBrokerInstance() = brokerInstances.random()
        private fun generateRandom() = abs(Random.nextLong())
        private fun newRandomTopic() = "topic${generateRandom()}"
        private fun newRandomMessage() = "message${generateRandom()}"

        @AfterAll
        @JvmStatic
        fun cleanUp() {
            brokerInstances.forEach { broker -> broker.shutdown() }
        }
    }
}
