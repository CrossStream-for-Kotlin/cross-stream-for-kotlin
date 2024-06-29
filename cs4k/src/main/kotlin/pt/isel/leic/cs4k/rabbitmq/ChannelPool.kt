package pt.isel.leic.cs4k.rabbitmq

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import java.io.Closeable
import java.io.IOException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.Continuation
import kotlin.coroutines.resumeWithException
import kotlin.time.Duration

/**
 * Manager of connections created from a single connection.
 *
 * @property connection The connection from which channels are created from.
 * @property maxChannels The maximum amount of channels that can be created at once.
 */
class ChannelPool(
    private val connection: Connection,
    private val maxChannels: Int = 1023
) : Closeable {

    init {
        // There can't be negative channel numbers.
        require(maxChannels > 0) { "Channel count must be higher than 0." }
    }

    // Lock to turn access to the pool thread-safe.
    private val lock = ReentrantLock()

    /**
     * Information related to the channel.
     *
     * @property channel The channel.
     * @property consumerTag The consumerTag linked with a consumption done in this channel. If null, then the channel
     * isn't being used for consuming.
     */
    private class ChannelEntry(
        val channel: Channel,
        var consumerTag: String? = null
    )

    // Flag that dictates if it is closed.
    var isClosed = false
        private set

    // Collection of created channels.
    private val channels: MutableList<ChannelEntry> = mutableListOf()

    /**
     * Request for a new channel.
     *
     * @property continuation Remainder of the code that is resumed when channel is obtained.
     * @property channel The channel that the requester will receive.
     */
    private class ChannelRequest(
        val continuation: Continuation<Unit>,
        var channel: Channel? = null
    )

    // List of requests for a channel.
    private val channelRequestList = mutableListOf<ChannelRequest>()

    /**
     * Obtain a new channel. If no channels are available and max capacity is reached, it will passively wait for a new
     * connection given by another thread.
     *
     * @return An unused channel, if timeout isn't reached.
     */
    private suspend fun getChannel(): Channel {
        var myRequest: ChannelRequest? = null
        var channel: Channel? = null
        try {
            suspendCancellableCoroutine<Unit> { continuation ->
                lock.withLock {
                    if (channels.count() == maxChannels) {
                        val request = ChannelRequest(continuation)
                        myRequest = request
                        channelRequestList.add(request)
                    } else {
                        val myChannel = connection.createChannel()
                        channel = myChannel
                        channels.add(ChannelEntry(myChannel))
                        continuation.resumeWith(Result.success(Unit))
                    }
                }
            }
        } catch (e: CancellationException) {
            if (myRequest?.channel == null) {
                lock.withLock {
                    channelRequestList.remove(myRequest)
                }
                throw e
            } else {
                return requireNotNull(myRequest?.channel)
            }
        }
        return channel ?: myRequest?.channel!!
    }

    /**
     * Obtain a new channel. If no channels are available and max capacity is reached, it will passively wait for a new
     * connection given by another thread or until timeout is reached.
     *
     * @param timeout Maximum amount of time waiting to fetch a new channel.
     * @return An unused channel, if timeout isn't reached.
     */
    fun getChannel(timeout: Duration = Duration.INFINITE): Channel {
        if (isClosed) {
            throw IOException("Pool already closed - cannot obtain new channels")
        }
        return runBlocking {
            var result: Channel? = null
            try {
                withTimeout(timeout) {
                    result = getChannel()
                    result!!
                }
            } catch (e: TimeoutCancellationException) {
                if (result == null) {
                    throw e
                } else {
                    result!!
                }
            }
        }
    }

    /**
     * Makes the channel free for the taking.
     *
     * @param channel The channel that the user doesn't want to use.
     */
    fun stopUsingChannel(channel: Channel) {
        if (isClosed) return
        lock.withLock {
            val channelInfo = channels.find {
                it.channel.connection.id == channel.connection.id && it.channel.channelNumber == channel.channelNumber
            }
            requireNotNull(channelInfo) { "Channel provided must have been created by the pool" }
            channel.close()
            channels.remove(channelInfo)
            if (channelRequestList.isNotEmpty()) {
                val entry = channelRequestList.removeFirst()
                entry.channel = connection.createChannel()
                entry.continuation.resumeWith(Result.success(Unit))
            }
        }
    }

    /**
     * Closes every channel and releases every waiting requester.
     */
    override fun close() = lock.withLock {
        if (!isClosed) {
            isClosed = true
            channelRequestList.forEach {
                it.continuation.resumeWithException(CancellationException("Pools have been closed."))
            }
            connection.close()
            channels.clear()
        }
    }
}
