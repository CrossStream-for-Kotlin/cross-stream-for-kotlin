package pt.isel.leic.cs4k.adaptar

import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import pt.isel.leic.cs4k.adapter.subscribeToFlow
import pt.isel.leic.cs4k.common.Event
import pt.isel.leic.cs4k.postgreSQL.BrokerPostgreSQL
import pt.isel.leic.cs4k.redis.BrokerRedis
import pt.isel.leic.cs4k.redis.RedisNode
import pt.isel.leic.cs4k.utils.Environment
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FlowBrokerAdapterTests {

    @Test
    fun `one subscriber collect last event of a finished topic via flow`() {
        // Arrange
        val broker = BrokerPostgreSQL(Environment.getPostgreSqlDbUrl())
        val topic = newRandomTopic()
        val message = newRandomMessage()

        // Act
        broker.publish(
            topic = topic,
            message = message,
            isLastMessage = true
        )

        // Assert
        runBlocking {
            this.launch {
                broker.subscribeToFlow(topic).collect { event ->
                    // Assert [1]
                    assertEquals(topic, event.topic)
                    assertEquals(0, event.id)
                    assertEquals(message, event.message)
                    assertTrue(event.isLast)
                }
            }
        }
    }

    @Test
    fun `one subscriber collect a set of events of a topic via flow`() {
        // Arrange
        val broker = BrokerPostgreSQL(Environment.getPostgreSqlDbUrl())
        val topic = newRandomTopic()
        val messages = List(NUMBER_OF_MESSAGES) { newRandomMessage() }

        val eventsReceived = ConcurrentLinkedQueue<Event>()

        // Act
        runBlocking {
            launch {
                broker.subscribeToFlow(topic).collect { event ->
                    // Assert [1]
                    eventsReceived.add(event)
                }
            }

            delay(2000)

            launch {
                messages.forEachIndexed { index, message ->
                    if (index == messages.size - 1) {
                        broker.publish(topic, message, true)
                    } else {
                        broker.publish(topic, message)
                    }
                }
            }
        }

        // Assert
        assertEquals(NUMBER_OF_MESSAGES, eventsReceived.count())
        assertEquals(messages, eventsReceived.map { it.message })
    }

    @Test
    fun `one subscriber collect a set of events of a set of topic via flow`() {
        // Arrange
        val broker = BrokerPostgreSQL(Environment.getPostgreSqlDbUrl())
        val topicsAndMessages = (1..NUMBER_OF_TOPICS).associate {
            newRandomTopic() to List(NUMBER_OF_MESSAGES) { newRandomMessage() }
        }

        val eventsReceived = ConcurrentLinkedQueue<Event>()

        // Act
        runBlocking {
            topicsAndMessages.forEach {
                launch {
                    broker.subscribeToFlow(it.key).collect { event ->
                        // Assert [1]
                        eventsReceived.add(event)
                    }
                }
            }

            delay(3000)

            launch {
                topicsAndMessages.forEach { entry ->
                    entry.value.forEachIndexed { index, message ->
                        if (index == NUMBER_OF_MESSAGES - 1) {
                            broker.publish(entry.key, message, true)
                        } else {
                            broker.publish(entry.key, message)
                        }
                    }
                }
            }
        }

        // Assert
        assertEquals(NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS, eventsReceived.count())
        topicsAndMessages.forEach { entry ->
            val messagesReceived = eventsReceived.filter { it.topic == entry.key }.map { it.message }
            assertEquals(entry.value, messagesReceived)
        }
    }

    @Test
    fun `n subscribers collect a set of events of a set of topic via flow`() {
        // Arrange
        val broker = BrokerPostgreSQL(Environment.getPostgreSqlDbUrl())
        val topicsAndMessages = (1..NUMBER_OF_TOPICS).associate {
            newRandomTopic() to List(NUMBER_OF_MESSAGES) { newRandomMessage() }
        }

        val eventsReceived = ConcurrentLinkedQueue<Event>()

        // Act
        runBlocking {
            repeat(NUMBER_OF_SUBSCRIBERS) {
                topicsAndMessages.forEach {
                    launch {
                        broker.subscribeToFlow(it.key).collect { event ->
                            // Assert [1]
                            eventsReceived.add(event)
                        }
                    }
                }
            }

            delay(3000)

            launch {
                topicsAndMessages.forEach { entry ->
                    entry.value.forEachIndexed { index, message ->
                        if (index == NUMBER_OF_MESSAGES - 1) {
                            broker.publish(entry.key, message, true)
                        } else {
                            broker.publish(entry.key, message)
                        }
                    }
                }
            }
        }

        // Assert
        assertEquals(NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS * NUMBER_OF_SUBSCRIBERS, eventsReceived.count())
        val eventsReceivedSet = eventsReceived.toSet().toList()

        assertEquals(NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS, eventsReceivedSet.count())
        topicsAndMessages.forEach { entry ->
            val messagesReceived = eventsReceivedSet.filter { it.topic == entry.key }.map { it.message }
            assertEquals(entry.value, messagesReceived)
        }
    }

    private companion object {
        private const val NUMBER_OF_MESSAGES = 100
        private const val NUMBER_OF_TOPICS = 10
        private const val NUMBER_OF_SUBSCRIBERS = 100

        private fun generateRandom() = abs(Random.nextLong())
        private fun newRandomTopic() = "topic${generateRandom()}"
        private fun newRandomMessage() = "message${generateRandom()}"
    }
}
