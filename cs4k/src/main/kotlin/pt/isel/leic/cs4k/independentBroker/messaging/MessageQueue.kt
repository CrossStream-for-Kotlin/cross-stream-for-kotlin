package pt.isel.leic.cs4k.independentBroker.messaging

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.time.Duration

/**
 * Thread-safe class for message communication between coroutines, through message queues.
 * Communication uses the FIFO (first in first out) criteria.
 *
 * @property capacity The maximum number of messages stored in queue.
 */
class MessageQueue<T>(private val capacity: Int) {

    init {
        require(capacity > 0) { "Capacity must be a positive number." }
    }

    private val messageQueue = mutableListOf<T>()

    private class EnqueueRequest<T>(
        val message: T,
        val continuation: Continuation<Unit>,
        var isDone: Boolean = false
    )

    private val enqueueRequests = mutableListOf<EnqueueRequest<T>>()

    private class DequeueRequest<T>(
        val continuation: Continuation<Unit>,
        var value: T? = null,
        var isDone: Boolean = false
    )

    private val dequeueRequests = mutableListOf<DequeueRequest<T>>()

    private val lock = ReentrantLock()

    /**
     * Deliver a message to the queue.
     *
     * @param message The message to be delivered.
     */
    suspend fun enqueue(message: T) {
        // Save dequeue request to resume continuation without being in possession of the lock.
        var dequeueRequest: DequeueRequest<T>? = null
        // Save my enqueue request.
        var myRequest: EnqueueRequest<T>? = null
        try {
            suspendCancellableCoroutine<Unit> { continuation ->
                lock.withLock {
                    if (enqueueRequests.isEmpty() && messageQueue.size < capacity) {
                        if (dequeueRequests.isNotEmpty()) {
                            dequeueRequest = dequeueRequests.removeAt(0)
                            dequeueRequest?.isDone = true
                            if (messageQueue.isNotEmpty()) {
                                dequeueRequest?.value = messageQueue.removeAt(0)
                                messageQueue.add(message)
                            } else {
                                dequeueRequest?.value = message
                            }
                        } else {
                            messageQueue.add(message)
                        }
                        continuation.resume(Unit)
                    } else {
                        myRequest = EnqueueRequest(message, continuation)
                        myRequest?.let { request -> enqueueRequests.add(request) }
                    }
                }
            }
        } catch (e: CancellationException) {
            if (myRequest?.isDone == true) {
                return
            } else {
                lock.withLock {
                    enqueueRequests.remove(myRequest)
                }
                throw e
            }
        }
        dequeueRequest?.continuation?.resume(Unit)
    }

    /**
     * Get a message from the queue with timeout.
     *
     * @param timeout The time limit to wait for the message to be available.
     */
    suspend fun dequeue(timeout: Duration): T {
        require(timeout.inWholeNanoseconds > 0L) { "Timeout must be greater than zero." }
        var res: T? = null
        return try {
            withTimeout(timeout) {
                res = dequeue()
                requireNotNull(res) { "The 'res' should not be null." }
            }
        } catch (e: CancellationException) {
            if (res != null) {
                requireNotNull(res) { "The 'res' should not be null." }
            } else {
                throw e
            }
        }
    }

    /**
     * Get a message from the queue without timeout.
     */
    private suspend fun dequeue(): T {
        // Save enqueue request to resume continuation without being in possession of the lock.
        var enqueueRequest: EnqueueRequest<T>? = null
        // Save my dequeue request or message.
        var myRequest: DequeueRequest<T>? = null
        var message: T? = null
        try {
            suspendCancellableCoroutine<Unit> { continuation ->
                lock.withLock {
                    if (dequeueRequests.isEmpty() && messageQueue.isNotEmpty()) {
                        message = messageQueue.removeAt(0)
                        if (enqueueRequests.isNotEmpty()) {
                            enqueueRequest = enqueueRequests.removeAt(0)
                            enqueueRequest?.isDone = true
                            enqueueRequest?.message?.let { message -> messageQueue.add(message) }
                        }
                        continuation.resume(Unit)
                    } else {
                        myRequest = DequeueRequest(continuation)
                        myRequest?.let { request -> dequeueRequests.add(request) }
                    }
                }
            }
        } catch (e: CancellationException) {
            if (myRequest?.isDone == true) {
                return requireNotNull(myRequest?.value) { "The 'myRequest.value' should not be null." }
            } else {
                lock.withLock {
                    dequeueRequests.remove(myRequest)
                }
                throw e
            }
        }
        enqueueRequest?.continuation?.resume(Unit)
        return message ?: requireNotNull(myRequest?.value) { "The 'myRequest.value' should not be null." }
    }
}
