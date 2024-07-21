package pt.isel.leic.cs4k.demo.services

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import pt.isel.leic.cs4k.Broker
import pt.isel.leic.cs4k.adapter.subscribeToFlow
import pt.isel.leic.cs4k.common.Event
import pt.isel.leic.cs4k.demo.domain.Message
import pt.isel.leic.cs4k.demo.domain.MessageQueue
import pt.isel.leic.cs4k.demo.http.models.output.MessageOutputModel
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Duration

@Component
class ChatService(val broker: Broker) {

    private val generalGroup = "general"

    private val sseEmittersToKeepAlive = mutableListOf<SseTracker>()
    private val lock = ReentrantLock()

    private val messageQueueFlow = MessageQueue<Triple<SseTracker, Flow<Event>, (Event) -> Unit>>(1000)
    private val th: Thread = Thread {
        runBlocking {
            while (true) {
                messageQueueFlow.dequeue(Duration.INFINITE).let { (sseTracker, flow, handler) ->
                    val job = this.launch {
                        flow.collect { event ->
                            logger.info("Collected event from flow -> {}", event)
                            handler(event)
                        }
                    }
                    sseTracker.job = job
                }
            }
        }
    }

    init {
        Executors.newScheduledThreadPool(1).also {
            it.scheduleAtFixedRate({ keepAlive() }, 2, 1, TimeUnit.SECONDS)
        }

        th.start()
    }

    /**
     * Join a chat group.
     * @param group The optional name of the group.
     * @return The Spring SSEEmitter.
     */
    fun newListener(group: String?, subscribedNode: String): SseEmitter {
        val sseEmitter = SseEmitter(TimeUnit.MINUTES.toMillis(30))
        lock.withLock { sseEmittersToKeepAlive.add(SseTracker(sseEmitter)) }

        val unsubscribeCallback = broker.subscribe(
            topic = group ?: generalGroup,
            handler = { event ->
                try {
                    val message =
                        if (event.topic != Broker.SYSTEM_TOPIC) {
                            val message = deserializeJsonToMessage(event.message)
                            MessageOutputModel(message.message, subscribedNode, message.publishingNode)
                        } else {
                            MessageOutputModel(event.message)
                        }
                    SseEvent.Message(
                        name = event.topic,
                        id = event.id,
                        data = message
                    ).writeTo(
                        sseEmitter
                    )

                    if (event.isLast) sseEmitter.complete()
                } catch (ex: Exception) {
                    sseEmitter.completeWithError(ex)
                }
            }
        )

        sseEmitter.onCompletion {
            unsubscribeCallback()
            lock.withLock { sseEmittersToKeepAlive.removeIf { it.sseEmitter == sseEmitter } }
        }
        sseEmitter.onError {
            unsubscribeCallback()
            lock.withLock { sseEmittersToKeepAlive.removeIf { it.sseEmitter == sseEmitter } }
        }

        return sseEmitter
    }

    fun newListenerFlow(group: String?, subscribedNode: String): SseEmitter {
        val sseEmitter = SseEmitter(TimeUnit.MINUTES.toMillis(30))
        val tracker = SseTracker(sseEmitter)
        lock.withLock { sseEmittersToKeepAlive.add(tracker) }

        val flow = broker.subscribeToFlow(group ?: generalGroup)
        runBlocking {
            messageQueueFlow.enqueue(
                Triple(tracker, flow) { event ->
                    try {
                        val message =
                            if (event.topic != Broker.SYSTEM_TOPIC) {
                                val message = deserializeJsonToMessage(event.message)
                                MessageOutputModel(message.message, subscribedNode, message.publishingNode)
                            } else {
                                MessageOutputModel(event.message)
                            }
                        SseEvent.Message(
                            name = event.topic,
                            id = event.id,
                            data = message
                        ).writeTo(sseEmitter)

                        if (event.isLast) sseEmitter.complete()
                    } catch (ex: Exception) {
                        sseEmitter.completeWithError(ex)
                    }
                }
            )
        }

        sseEmitter.onCompletion {
            lock.withLock {
                sseEmittersToKeepAlive.remove(tracker)
                tracker.job?.cancel()
            }
        }
        sseEmitter.onError {
            lock.withLock {
                sseEmittersToKeepAlive.remove(tracker)
                tracker.job?.cancel()
            }
        }

        return sseEmitter
    }

    /**
     * Send a message to a group.
     * @param group The optional name of the group.
     * @param message The message to send to the group.
     */
    fun sendMessage(group: String?, message: String, publishingNode: String) {
        broker.publish(
            topic = group ?: generalGroup,
            message = serializeMessageToJson(Message(message, publishingNode))
        )
    }

    /**
     * Get system topic.
     * @return The Broker system topic.
     */
    fun getSystemTopic() = Broker.SYSTEM_TOPIC

    /**
     * Send a keep alive to all active sseEmitters.
     */
    private fun keepAlive() = lock.withLock {
        val keepAlive = SseEvent.KeepAlive(Instant.now().epochSecond)
        sseEmittersToKeepAlive.forEach { sseTracker ->
            try {
                keepAlive.writeTo(sseTracker.sseEmitter)
            } catch (ex: Exception) {
                // Ignore
            }
        }
    }

    /**
     * Spring shutdown hook.
     */
    @PreDestroy
    private fun clanUp() {
        broker.shutdown()
        th.interrupt()
        th.join()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ChatService::class.java)

        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        private fun serializeMessageToJson(message: Message) =
            objectMapper.writeValueAsString(message)

        private fun deserializeJsonToMessage(message: String) =
            objectMapper.readValue(message, Message::class.java)
    }
}
