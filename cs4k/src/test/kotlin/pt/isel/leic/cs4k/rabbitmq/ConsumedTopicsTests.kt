package pt.isel.leic.cs4k.rabbitmq

import org.junit.jupiter.api.Test
import pt.isel.leic.cs4k.common.Event
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class ConsumedTopicsTests {

    @Test
    fun `after a full update, can get all info stored and get latest offset`() {
        val allInfo = List(5) {
            ConsumedTopics.ConsumeInfo(it.toLong(), Event("topic$it", 0, "message"))
        }
        val topics = ConsumedTopics()
        topics.fullUpdate(allInfo)
        val events = topics.getAllLatestEventInfos()
        assertContentEquals(allInfo, events)
        assertEquals(4, topics.getMaximumOffset())
    }

    @Test
    fun `concurrent reads can read the same maximum offset when defined`() {
        val failures = ConcurrentLinkedQueue<AssertionError>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val topics = ConsumedTopics()
        val offset = 0L
        val event = Event("topic", 0L, "message")
        val readThreads = listOf(
            Thread {
                try {
                    val readOffset = topics.getMaximumOffset()
                    assertEquals(offset, readOffset)
                } catch (e: AssertionError) {
                    failures.add(e)
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
        )
        readThreads.forEach { it.start() }
        topics.fullUpdate(listOf(ConsumedTopics.ConsumeInfo(offset, event)))
        readThreads.forEach { it.join() }
        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()

        assertEquals(0L, topics.getMaximumOffset())
    }

    @Test
    fun `can create event from message and store it, able to retrieve later`() {
        val topics = ConsumedTopics()
        val event = topics.createAndSetLatestEventAndOffset("topic", 0L, "message")
        assertEquals("topic", event.topic)
        assertEquals(0L, event.id)
        assertEquals(0L, topics.getMaximumOffset())
        assertEquals("message", event.message)
        val storedEvent = topics.getLatestEvent("topic")
        assertEquals(event, storedEvent)
    }
}
