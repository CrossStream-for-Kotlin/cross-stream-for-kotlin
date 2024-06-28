package pt.isel.leic.cs4k.independent.messaging

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.coroutines.cancellation.CancellationException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds

class MessageQueueTests {

    private val waitTime = 10.seconds

    @Test
    fun `enqueue and dequeue single message`() = runBlocking {
        val queue = MessageQueue<Int>(capacity = 10)
        queue.enqueue(1)
        val message = queue.dequeue(waitTime)
        assertEquals(1, message)
    }

    @Test
    fun `enqueue multiple messages and dequeue`() = runBlocking {
        val queue = MessageQueue<Int>(capacity = 10)
        queue.enqueue(1)
        queue.enqueue(2)
        queue.enqueue(3)

        assertEquals(1, queue.dequeue(waitTime))
        assertEquals(2, queue.dequeue(waitTime))
        assertEquals(3, queue.dequeue(waitTime))
    }

    @Test
    fun `dequeue with timeout`() = runBlocking {
        val queue = MessageQueue<Int>(capacity = 10)

        val job = launch {
            delay(500)
            queue.enqueue(1)
        }

        val message = queue.dequeue(waitTime)
        assertEquals(1, message)
        job.join()
    }

    @Test
    fun `enqueue when queue is full`() = runBlocking {
        val queue = MessageQueue<Int>(capacity = 2)
        queue.enqueue(1)
        queue.enqueue(2)

        val job = launch {
            queue.enqueue(3)
        }

        delay(500)
        assertEquals(1, queue.dequeue(waitTime))
        job.join()
        assertEquals(2, queue.dequeue(waitTime))
        assertEquals(3, queue.dequeue(waitTime))
    }

    @Test
    fun `dequeue when queue is empty`() = runBlocking {
        val queue = MessageQueue<Int>(capacity = 2)

        val job = launch {
            delay(500)
            queue.enqueue(1)
        }

        val message = queue.dequeue(waitTime)
        assertEquals(1, message)
        job.join()
    }

    @Test
    fun `messageQueue requires`() {
        assertFailsWith<IllegalArgumentException> { MessageQueue<Int>(0) }
        runBlocking {
            val messageQueue = MessageQueue<Int>(1)
            assertFailsWith<IllegalArgumentException> { messageQueue.dequeue(0.seconds) }
        }
    }

    @Test
    fun `messageQueue successful`() {
        runBlocking {
            val messageQueue = MessageQueue<Int>(1)
            messageQueue.enqueue(5)
            assertEquals(5, messageQueue.dequeue(waitTime))
        }
    }

    @Test
    fun `messageQueue successful with multiple messages FIFO`() {
        runBlocking {
            val messageQueue = MessageQueue<Int>(3)
            messageQueue.enqueue(5)
            messageQueue.enqueue(6)
            messageQueue.enqueue(7)
            assertEquals(5, messageQueue.dequeue(waitTime))
            assertEquals(6, messageQueue.dequeue(waitTime))
            assertEquals(7, messageQueue.dequeue(waitTime))
        }
    }

    @Test
    fun `messageQueue successful with multiple coroutines`() {
        runBlocking {
            val messageQueue = MessageQueue<Int>(10)
            val coroutines = List(10) {
                launch {
                    messageQueue.enqueue(it)
                }
            }
            coroutines.forEach {
                it.join()
            }
            assertEquals(0, messageQueue.dequeue(waitTime))
        }
    }

    @Test
    fun `messageQueue successful with multiple coroutines enqueuing and then dequeuing`() {
        runBlocking {
            val messageQueueSize = 10
            val values = 50
            val messageQueue = MessageQueue<Int>(messageQueueSize)
            val messages = ConcurrentLinkedQueue<Int>()
            runBlocking {
                repeat(values) {
                    launch {
                        messageQueue.enqueue(it)
                    }
                }
                repeat(values) {
                    launch {
                        messages.add(messageQueue.dequeue(waitTime))
                    }
                }
            }
            assertEquals((0 until values).toList(), messages.toList())
        }
    }

    @Test
    fun `messageQueue successful with multiple coroutines simultaneously enqueuing and dequeuing`() {
        runBlocking {
            val messageQueueSize = 1
            val values = 5000
            val messageQueue = MessageQueue<Int>(messageQueueSize)
            val messages = ConcurrentLinkedQueue<Int>()
            runBlocking {
                repeat(values) {
                    launch {
                        messageQueue.enqueue(it)
                        messages.add(messageQueue.dequeue(waitTime))
                    }
                }
            }
            assertEquals((0 until values).toList(), messages.toList())
        }
    }

    @Test
    fun `cancellation dequeue`() {
        runBlocking {
            val messageQueue = MessageQueue<Int>(1)
            val job = launch {
                assertFailsWith<CancellationException> { messageQueue.dequeue(waitTime) }
            }
            job.cancel()
        }
    }

    @Test
    fun `cancellation enqueue`() {
        runBlocking {
            val messageQueue = MessageQueue<Int>(1)
            val job = launch {
                messageQueue.enqueue(5)
                assertFailsWith<CancellationException> { messageQueue.enqueue(6) }
            }
            job.cancel()
        }
    }

    @Test
    fun `cancellation due to dequeue timeout`() {
        runBlocking {
            val messageQueue = MessageQueue<Int>(1)
            assertFailsWith<TimeoutCancellationException> { messageQueue.dequeue(2.seconds) }
        }
    }
}
