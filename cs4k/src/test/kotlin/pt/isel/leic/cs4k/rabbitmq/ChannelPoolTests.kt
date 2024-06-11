package pt.isel.leic.cs4k.rabbitmq

import com.rabbitmq.client.Address
import com.rabbitmq.client.ConnectionFactory
import kotlinx.coroutines.TimeoutCancellationException
import org.junit.jupiter.api.Test
import pt.isel.leic.cs4k.utils.Environment
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.concurrent.thread
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class ChannelPoolTests {

    @Test
    fun `can create channel from pool and close it afterwards, followed by closing the pool`() {
        val singleConnection = factory.newConnection(
            listOf(Address(Environment.getRabbitHost(), Environment.getRabbitPort()))
        )
        val pool = ChannelPool(singleConnection)
        assertTrue { !pool.isClosed }
        val channel = pool.getChannel()
        assertTrue { channel.isOpen }
        pool.stopUsingChannel(channel)
        assertTrue { !channel.isOpen }
        pool.close()
        assertTrue { pool.isClosed }
        assertTrue { !singleConnection.isOpen }
    }

    @Test
    fun `multiple channels can be created, and are closed at the same time when the pool is closed`() {
        val singleConnection = factory.newConnection(
            listOf(Address(Environment.getRabbitHost(), Environment.getRabbitPort()))
        )
        val pool = ChannelPool(singleConnection)
        assertTrue { !pool.isClosed }
        val channels = List(5) { pool.getChannel() }
        channels.forEach { assertTrue { it.isOpen } }
        pool.close()
        assertTrue { pool.isClosed }
        channels.forEach { assertTrue { !it.isOpen } }
        assertTrue { !singleConnection.isOpen }
    }

    @Test
    fun `when size limit is reached, thread will wait until channel is freed for a new channel`() {
        val failures = ConcurrentLinkedQueue<AssertionError>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val singleConnection = factory.newConnection(
            listOf(Address(Environment.getRabbitHost(), Environment.getRabbitPort()))
        )
        val pool = ChannelPool(singleConnection, 1)
        assertTrue { !pool.isClosed }
        val channel = pool.getChannel()
        val thread = thread {
            try {
                val threadChannel = pool.getChannel(5.seconds)
                assertTrue { threadChannel.isOpen }
            } catch (e: AssertionError) {
                failures.add(e)
            } catch (e: Exception) {
                errors.add(e)
            }
        }
        Thread.sleep(1000)
        pool.stopUsingChannel(channel)
        thread.join()
        pool.close()
        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
        assertTrue { pool.isClosed }
        assertTrue { !channel.isOpen }
        assertTrue { !singleConnection.isOpen }
    }

    @Test
    fun `when size limit is reached, thread will wait until timeout for a new channel`() {
        val singleConnection = factory.newConnection(
            listOf(Address(Environment.getRabbitHost(), Environment.getRabbitPort()))
        )
        val pool = ChannelPool(singleConnection, 1)
        assertTrue { !pool.isClosed }
        val channel = pool.getChannel()
        assertFailsWith<TimeoutCancellationException> { pool.getChannel(5.seconds) }
        pool.close()
        assertTrue { pool.isClosed }
        assertTrue { !channel.isOpen }
        assertTrue { !singleConnection.isOpen }
    }

    @Test
    fun `illegal channel limits`() {
        val singleConnection = factory.newConnection(
            listOf(Address(Environment.getRabbitHost(), Environment.getRabbitPort()))
        )
        assertFailsWith<IllegalArgumentException> { ChannelPool(singleConnection, -1) }
        assertFailsWith<IllegalArgumentException> { ChannelPool(singleConnection, 0) }
    }

    companion object {
        val factory = ConnectionFactory()
    }
}
