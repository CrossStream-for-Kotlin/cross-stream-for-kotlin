package pt.isel.leic.cs4k.common

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class AssociatedSubscribersTests {

    @Test
    fun `insert a subscribe`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriberId = UUID.randomUUID()
        val subscriber = Subscriber(subscriberId) { _ -> }

        // Act
        associatedSubscribers.addToKey(topic, subscriber)
        val subscribers = associatedSubscribers.getAll(topic)

        // Assert
        assertFalse(associatedSubscribers.noSubscribers(topic))
        assertEquals(1, subscribers.size)
        assertEquals(subscriber, subscribers.first())
    }

    @Test
    fun `insert a subscribe with event tracking`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriberId = UUID.randomUUID()
        val subscriber = SubscriberWithEventTracking(subscriberId, { _ -> })

        // Act [1]
        associatedSubscribers.addToKey(topic, subscriber)
        val subscribers = associatedSubscribers.getAndUpdateAll(topic, 1_000_000)

        // Assert [1]
        assertFalse(associatedSubscribers.noSubscribers(topic))
        assertEquals(1, subscribers.size)
        assertEquals(subscriber, subscribers.first())
        assertNull((subscribers.first() as SubscriberWithEventTracking).lastEventIdReceived)

        // Act [2]
        val subscribers2 = associatedSubscribers.getAndUpdateAll(topic, 1_000_001)

        // Assert [2]
        assertFalse(associatedSubscribers.noSubscribers(topic))
        assertEquals(1, subscribers2.size)
        assertNotEquals(subscriber, subscribers2.first())
        assertNotNull((subscribers2.first() as SubscriberWithEventTracking).lastEventIdReceived)
        assertEquals(1_000_000, (subscribers2.first() as SubscriberWithEventTracking).lastEventIdReceived)

        // Act [3]
        val subscribers3 = associatedSubscribers.getAndUpdateAll(topic, 1_000_001)
        val subscribers4 = associatedSubscribers.getAll(topic)

        // Assert [3]
        assertFalse(associatedSubscribers.noSubscribers(topic))
        assertEquals(0, subscribers3.size)
        assertEquals(1, subscribers4.size)
        assertEquals(1_000_001, (subscribers4.first() as SubscriberWithEventTracking).lastEventIdReceived)
    }

    @Test
    fun `insert and remove a subscribe`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriberId = UUID.randomUUID()
        val subscriber = Subscriber(subscriberId) { _ -> }

        // Act [1]
        associatedSubscribers.addToKey(topic, subscriber)

        // Assert [1]
        assertFalse(associatedSubscribers.noSubscribers(topic))
        assertEquals(1, associatedSubscribers.getAll(topic).size)

        // Act [2]
        associatedSubscribers.removeIf(topic, { it.id == subscriberId })

        // Assert [2]
        assertTrue(associatedSubscribers.noSubscribers(topic))
    }

    @Test
    fun `insert and remove a subscribe with event tracking`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriberId = UUID.randomUUID()
        val subscriber = SubscriberWithEventTracking(subscriberId, { _ -> })

        // Act [1]
        associatedSubscribers.addToKey(topic, subscriber)

        // Assert [1]
        assertFalse(associatedSubscribers.noSubscribers(topic))
        assertEquals(1, associatedSubscribers.getAndUpdateAll(topic, 1).size)

        // Act [2]
        associatedSubscribers.removeIf(topic, { it.id == subscriberId })

        // Assert [2]
        assertTrue(associatedSubscribers.noSubscribers(topic))
    }

    @Test
    fun `insert two subscribers and remove the first one`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = Subscriber(subscriberId1) { _ -> }
        val subscriber2 = Subscriber(subscriberId2) { _ -> }

        // Act
        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)
        associatedSubscribers.removeIf(topic, { it.id == subscriberId1 })
        val subscribers = associatedSubscribers.getAll(topic)

        // Assert
        assertFalse(associatedSubscribers.noSubscribers(topic))
        assertEquals(1, subscribers.size)
        assertEquals(subscriber2, subscribers.first())
    }

    @Test
    fun `insert two subscribers with event tracking and remove the first one`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = SubscriberWithEventTracking(subscriberId1, { _ -> })
        val subscriber2 = SubscriberWithEventTracking(subscriberId2, { _ -> })

        // Act
        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)
        associatedSubscribers.removeIf(topic, { it.id == subscriberId1 })
        val subscribers = associatedSubscribers.getAndUpdateAll(topic, 1_000_000)
        val subscribers1 = associatedSubscribers.getAll(topic)

        // Assert
        assertFalse(associatedSubscribers.noSubscribers(topic))
        assertEquals(1, subscribers.size)
        assertEquals(subscriber2, subscribers.first())
        assertEquals(1_000_000, (subscribers1.first() as SubscriberWithEventTracking).lastEventIdReceived)
    }

    @Test
    fun `insert two subscribers and remove the last one`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = Subscriber(subscriberId1) { _ -> }
        val subscriber2 = Subscriber(subscriberId2) { _ -> }

        // Act
        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)
        associatedSubscribers.removeIf(topic, { it.id == subscriberId2 })
        val subscribers = associatedSubscribers.getAll(topic)

        // Assert
        assertFalse(associatedSubscribers.noSubscribers(topic))
        assertEquals(1, subscribers.size)
        assertEquals(subscriber1, subscribers.first())
    }

    @Test
    fun `insert two subscribers with event tracking and remove the last one`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = SubscriberWithEventTracking(subscriberId1, { _ -> })
        val subscriber2 = SubscriberWithEventTracking(subscriberId2, { _ -> })

        // Act
        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)
        associatedSubscribers.removeIf(topic, { it.id == subscriberId2 })
        val subscribers = associatedSubscribers.getAndUpdateAll(topic, 1_000_000)

        // Assert
        assertFalse(associatedSubscribers.noSubscribers(topic))
        assertEquals(1, subscribers.size)
        assertEquals(subscriber1, subscribers.first())
    }

    @Test
    fun `insert two subscribes and remove both in different threads`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = Subscriber(subscriberId1) { _ -> }
        val subscriber2 = Subscriber(subscriberId2) { _ -> }

        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)

        // Act
        val thread1 = Thread { associatedSubscribers.removeIf(topic, { it.id == subscriberId1 }) }
        val thread2 = Thread { associatedSubscribers.removeIf(topic, { it.id == subscriberId2 }) }
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        // Assert
        assertTrue(associatedSubscribers.noSubscribers(topic))
        assertEquals(0, associatedSubscribers.getAll(topic).size)
    }

    @Test
    fun `insert two subscribes with event tracking and remove both in different threads`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = SubscriberWithEventTracking(subscriberId1, { _ -> })
        val subscriber2 = SubscriberWithEventTracking(subscriberId2, { _ -> })

        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)

        // Act
        val thread1 = Thread { associatedSubscribers.removeIf(topic, { it.id == subscriberId1 }) }
        val thread2 = Thread { associatedSubscribers.removeIf(topic, { it.id == subscriberId2 }) }
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        // Assert
        assertTrue(associatedSubscribers.noSubscribers(topic))
        assertEquals(0, associatedSubscribers.getAndUpdateAll(topic, 1_000_000).size)
    }

    @Test
    fun `adding multiple subscribers to the same topic in different threads`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscribers = mutableListOf<Subscriber>()
        val threads = mutableListOf<Thread>()

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val subscriber = Subscriber(UUID.randomUUID()) { _ -> }
            subscribers.add(subscriber)
            val thread = Thread { associatedSubscribers.addToKey(topic, subscriber) }
            threads.add(thread)
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }

        // Assert
        val storedSubscribers = associatedSubscribers.getAll(topic)
        assertEquals(NUMBER_OF_SUBSCRIBERS, storedSubscribers.size)
        subscribers.forEach { subscriber ->
            assertContains(storedSubscribers, subscriber)
        }
    }

    @Test
    fun `adding multiple subscribers with event tracking to the same topic in different threads`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscribers = mutableListOf<SubscriberWithEventTracking>()
        val threads = mutableListOf<Thread>()

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val subscriber = SubscriberWithEventTracking(UUID.randomUUID(), { _ -> })
            subscribers.add(subscriber)
            val thread = Thread { associatedSubscribers.addToKey(topic, subscriber) }
            threads.add(thread)
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }

        // Assert [1]
        val storedSubscribers = associatedSubscribers.getAndUpdateAll(topic, 1_000_000)
        assertEquals(NUMBER_OF_SUBSCRIBERS, storedSubscribers.size)
        subscribers.forEach { subscriber ->
            assertContains(storedSubscribers, subscriber)
        }
    }

    @Test
    fun `adding multiple subscribers to a different topic in different threads`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topics = List(NUMBER_OF_TOPICS) { newTopic() }
        val subscribers = mutableListOf<Pair<String, Subscriber>>()
        val threads = mutableListOf<Thread>()

        // Act
        topics.forEach { topic ->
            repeat(NUMBER_OF_SUBSCRIBERS) {
                val subscriber = Subscriber(UUID.randomUUID()) { _ -> }
                subscribers.add(Pair(topic, subscriber))
                val thread = Thread { associatedSubscribers.addToKey(topic, subscriber) }
                threads.add(thread)
            }
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }

        // Assert
        subscribers.forEach { pair ->
            val storedSubscribers = associatedSubscribers.getAll(pair.first)
            assertEquals(NUMBER_OF_SUBSCRIBERS, storedSubscribers.size)
            assertContains(storedSubscribers, pair.second)
        }
    }

    @Test
    fun `adding and then removing multiple subscribers to the same topic in different threads`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscribers = ConcurrentLinkedQueue<Subscriber>()
        val threads = mutableListOf<Thread>()

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val subscriber = Subscriber(UUID.randomUUID()) { _ -> }
            subscribers.add(subscriber)
            val thread = Thread { associatedSubscribers.addToKey(topic, subscriber) }
            threads.add(thread)
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }
        threads.clear()

        repeat(NUMBER_OF_SUBSCRIBERS / 2) {
            val thread = Thread {
                val subscriber = subscribers.poll()
                associatedSubscribers.removeIf(topic, { it.id == subscriber.id })
            }
            threads.add(thread)
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }

        // Assert
        val storedSubscribers = associatedSubscribers.getAll(topic)
        assertEquals(NUMBER_OF_SUBSCRIBERS / 2, storedSubscribers.size)
        subscribers.forEach { subscriber ->
            assertContains(storedSubscribers, subscriber)
        }
    }

    @Test
    fun `adding and then removing multiple subscribers with event tracking to the same topic in different threads`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscribers = ConcurrentLinkedQueue<SubscriberWithEventTracking>()
        val threads = mutableListOf<Thread>()

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val subscriber = SubscriberWithEventTracking(UUID.randomUUID(), { _ -> })
            subscribers.add(subscriber)
            val thread = Thread { associatedSubscribers.addToKey(topic, subscriber) }
            threads.add(thread)
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }
        threads.clear()

        repeat(NUMBER_OF_SUBSCRIBERS / 2) {
            val thread = Thread {
                val subscriber = subscribers.poll()
                associatedSubscribers.removeIf(topic, { it.id == subscriber.id })
            }
            threads.add(thread)
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }

        // Assert
        val storedSubscribers = associatedSubscribers.getAndUpdateAll(topic, 10000)
        assertEquals(NUMBER_OF_SUBSCRIBERS / 2, storedSubscribers.size)
        subscribers.forEach { subscriber ->
            assertContains(storedSubscribers, subscriber)
        }
    }

    @Test
    fun `adding and then removing multiple subscribers to a different topic in different threads`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topics = List(NUMBER_OF_TOPICS) { newTopic() }
        val subscribers = ConcurrentLinkedQueue<Pair<String, Subscriber>>()
        val threads = mutableListOf<Thread>()

        // Act
        topics.forEach { topic ->
            repeat(NUMBER_OF_SUBSCRIBERS) {
                val subscriber = Subscriber(UUID.randomUUID()) { _ -> }
                subscribers.add(Pair(topic, subscriber))
                val thread = Thread { associatedSubscribers.addToKey(topic, subscriber) }
                threads.add(thread)
            }
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }
        threads.clear()

        topics.forEach { topic ->
            repeat(NUMBER_OF_SUBSCRIBERS / 2) {
                val thread = Thread {
                    val topicAndSubscriber = subscribers.poll()
                    associatedSubscribers.removeIf(topic, { it.id == topicAndSubscriber.second.id })
                }
                threads.add(thread)
            }
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }

        // Assert
        subscribers.forEach { pair ->
            val storedSubscribers = associatedSubscribers.getAll(pair.first)
            assertEquals(NUMBER_OF_SUBSCRIBERS, storedSubscribers.size)
            assertContains(storedSubscribers, pair.second)
        }
    }

    @Test
    fun `execute an action if subscriber subscribes to a new topic`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriber1 = Subscriber(UUID.randomUUID()) { _ -> }
        val subscriber2 = Subscriber(UUID.randomUUID()) { _ -> }

        var topicAdd = false
        var topicNotAdd = true

        // Act [1]
        associatedSubscribers.addToKey(topic, subscriber1) { topicAdd = true }

        // Assert [1]
        assertTrue(topicAdd)

        // Act [2]
        associatedSubscribers.addToKey(topic, subscriber2) { topicNotAdd = false }

        // Assert [2]
        assertTrue(topicNotAdd)
    }

    @Test
    fun `execute an action if there are no more subscribers to the topic`() {
        // Arrange
        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriber1 = Subscriber(UUID.randomUUID()) { _ -> }
        val subscriber2 = Subscriber(UUID.randomUUID()) { _ -> }
        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)

        var topicNotGone = true
        var topicGone = false

        // Act [1]
        associatedSubscribers.removeIf(topic, { it.id == subscriber1.id }, { topicNotGone = false })

        // Assert [1]
        assertTrue(topicNotGone)

        // Act [2]
        associatedSubscribers.removeIf(topic, { it.id == subscriber2.id }, { topicGone = true })

        // Assert [2]
        assertTrue(topicGone)
    }

    @Test
    fun `update the last event id received by a subscriber stored`() {
        // Arrange
        val initialLastEventID = 5L
        val updatedLastEventID = 6L

        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriber = SubscriberWithEventTracking(UUID.randomUUID(), { _ -> }, initialLastEventID)

        // Act [1]
        associatedSubscribers.addToKey(topic, subscriber)

        // Assert [1]
        assertEquals(
            initialLastEventID,
            (associatedSubscribers.getAll(topic).first() as SubscriberWithEventTracking).lastEventIdReceived
        )

        // Act [2]
        associatedSubscribers.updateLastEventIdReceived(topic, subscriber.id, updatedLastEventID)

        // Assert [2]
        assertEquals(
            updatedLastEventID,
            (associatedSubscribers.getAll(topic).first() as SubscriberWithEventTracking).lastEventIdReceived
        )
    }

    @Test
    fun `update and get a subscriber with event tracking`() {
        // Arrange
        val initialLastEventID = 5L
        val updatedLastEventID = 6L

        val associatedSubscribers = AssociatedSubscribers()
        val topic = newTopic()
        val subscriber = SubscriberWithEventTracking(UUID.randomUUID(), { _ -> }, initialLastEventID)

        // Act [1]
        associatedSubscribers.addToKey(topic, subscriber)

        // Assert [1]
        assertEquals(
            initialLastEventID,
            (associatedSubscribers.getAndUpdateAll(topic, 10L).first() as SubscriberWithEventTracking).lastEventIdReceived
        )
        assertEquals(0, associatedSubscribers.getAndUpdateAll(topic, 10L).size)
        assertEquals(1, associatedSubscribers.getAll(topic).size)
        assertEquals(10L, (associatedSubscribers.getAll(topic).first() as SubscriberWithEventTracking).lastEventIdReceived)

        // Act [2]
        associatedSubscribers.updateLastEventIdReceived(topic, subscriber.id, updatedLastEventID)

        // Assert [2]
        assertEquals(
            updatedLastEventID,
            (associatedSubscribers.getAndUpdateAll(topic, 10L).first() as SubscriberWithEventTracking).lastEventIdReceived
        )
        assertEquals(0, associatedSubscribers.getAndUpdateAll(topic, 10L).size)
        assertEquals(1, associatedSubscribers.getAll(topic).size)
        assertEquals(10L, (associatedSubscribers.getAll(topic).first() as SubscriberWithEventTracking).lastEventIdReceived)
    }

    private companion object {

        private const val NUMBER_OF_TOPICS = 200
        private const val NUMBER_OF_SUBSCRIBERS = 500

        private fun newTopic() = "topic${abs(Random.nextLong())}"
    }
}
