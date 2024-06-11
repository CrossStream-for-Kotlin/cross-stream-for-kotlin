package pt.isel.leic.cs4k.rabbitmq

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import pt.isel.leic.cs4k.common.Event
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.Continuation
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration

/**
 * Container that stored information of all consumed topics.
 */
class ConsumedTopics {

    /**
     * Information about events being consumed from the stream.
     * @property offset The offset of the message in the stream.
     * @property lastEvent The most recent event made from the consumed message.
     */
    data class ConsumeInfo(
        val offset: Long,
        val lastEvent: Event
    )

    // All information is stored here.
    private val consumeInfoMap: HashMap<String, ConsumeInfo> = hashMapOf()

    // Lock to control access.
    private val lock = ReentrantLock()

    /**
     * Structure of an offset request.
     * @param continuation Remainder of the code that is resumed when offset is obtained.
     * @param offset The offset of the last event of the topic.
     */
    private class OffsetRequest(
        val continuation: Continuation<Unit>,
        var offset: Long? = null
    )

    // All requests.
    private val offsetRequestList = mutableListOf<OffsetRequest>()

    /**
     * Create and set the latest event in a topic.
     * @param topic The topic of the event.
     * @param message The message of the event.
     * @param isLast If the event in question is the last of a given topic.
     * @return The newly created event.
     */
    fun createAndSetLatestEventAndOffset(topic: String, offset: Long, message: String, isLast: Boolean = false): Event =
        lock.withLock {
            val event = consumeInfoMap[topic]?.let { info ->
                val id = info.lastEvent.id + 1
                Event(topic, id, message, isLast)
            } ?: Event(topic, 0, message, isLast)
            consumeInfoMap[topic] = ConsumeInfo(offset, event)
            event
        }

    /**
     * Obtain all information that is currently stored.
     * @return All information stored.
     */
    fun getAllLatestEventInfos(): List<ConsumeInfo> = lock.withLock {
        val info = consumeInfoMap.values
        val maximumOffset = info.maxOfOrNull { it.offset }
        offsetRequestList.forEach {
            it.offset = maximumOffset
            it.continuation.resumeWith(Result.success(Unit))
        }
        return info.toList().sortedBy { it.offset }
    }

    /**
     * Full populates the map with information regarding certain topics.
     * @param events New information about topics.
     */
    fun fullUpdate(events: List<ConsumeInfo>) = lock.withLock {
        events.sortedBy { it.offset }.forEach { eventEntry ->
            consumeInfoMap[eventEntry.lastEvent.topic] =
                ConsumeInfo(
                    offset = eventEntry.offset,
                    lastEvent = eventEntry.lastEvent
                )
        }
    }

    /**
     * Reading the latest offset stored.
     * If there are no available offsets, then it will passively wait until notified.
     * @return The latest offset available.
     */
    private suspend fun getMaximumOffset(): Long? {
        var myRequest: OffsetRequest? = null
        var offset: Long? = null
        try {
            suspendCancellableCoroutine<Unit> { continuation ->
                lock.withLock {
                    val memory = consumeInfoMap.values
                    if (memory.isNotEmpty()) {
                        offset = memory.maxOfOrNull { value -> value.offset }
                        continuation.resumeWith(Result.success(Unit))
                    } else {
                        myRequest = OffsetRequest(continuation)
                        myRequest?.let { req ->
                            offsetRequestList.add(req)
                        }
                    }
                }
            }
        } catch (e: CancellationException) {
            if (myRequest?.offset != null) {
                return requireNotNull(myRequest?.offset)
            } else {
                lock.withLock {
                    offsetRequestList.remove(myRequest)
                }
            }
            throw e
        }
        return offset ?: myRequest?.offset
    }

    /**
     * Reading the latest offset stored.
     * If there are no available offsets, then it will passively wait until notified or until timeout is reached.
     * @param timeout Maximum amount of wait tine.
     * @return The latest offset, if able to be obtained.
     */
    fun getMaximumOffset(timeout: Duration = Duration.INFINITE): Long? {
        return runBlocking {
            var result: Long? = null
            try {
                withTimeout(timeout) {
                    result = getMaximumOffset()
                    result
                }
            } catch (e: CancellationException) {
                result
            }
        }
    }

    /**
     * Obtain the latest topic. When both sent and received are defined, received is prioritized.
     * @param topic The topic of the event desired.
     * @return The latest event of the topic.
     */
    fun getLatestEvent(topic: String): Event? = lock.withLock {
        consumeInfoMap[topic]?.lastEvent
    }

    /**
     * Removing all information stored.
     */
    fun removeAll() = lock.withLock {
        consumeInfoMap.clear()
    }
}
