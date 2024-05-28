package pt.isel.leic.cs4k.demo.services

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import jakarta.annotation.PreDestroy
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import pt.isel.leic.cs4k.demo.domain.Message
import pt.isel.leic.cs4k.demo.http.models.output.MessageOutputModel
import pt.isel.leic.cs4k.postgreSQL.BrokerPostgreSQL
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Component
class ChatService(val broker: BrokerPostgreSQL) {

    private val generalGroup = "general"

    private val sseEmittersToKeepAlive = mutableListOf<SseEmitter>()
    private val lock = ReentrantLock()

    init {
        Executors.newScheduledThreadPool(1).also {
            it.scheduleAtFixedRate({ keepAlive() }, 2, 1, TimeUnit.SECONDS)
        }
    }

    /**
     * Join a chat group.
     * @param group The optional name of the group.
     * @return The Spring SSEEmitter.
     */
    fun newListener(group: String?): SseEmitter {
        val sseEmitter = SseEmitter(TimeUnit.MINUTES.toMillis(30))
        lock.withLock { sseEmittersToKeepAlive.add(sseEmitter) }

        val unsubscribeCallback = broker.subscribe(
            topic = group ?: generalGroup,
            handler = { event ->
                try {
                    val messageReceived = deserializeJsonToMessage(event.message)
                    SseEvent.Message(
                        name = event.topic,
                        id = event.id,
                        data = MessageOutputModel(messageReceived.message)
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
            lock.withLock { sseEmittersToKeepAlive.remove(sseEmitter) }
        }
        sseEmitter.onError {
            unsubscribeCallback()
            lock.withLock { sseEmittersToKeepAlive.remove(sseEmitter) }
        }

        return sseEmitter
    }

    /**
     * Send a message to a group.
     * @param group The optional name of the group.
     * @param message The message to send to the group.
     */
    fun sendMessage(group: String?, message: String) {
        broker.publish(
            topic = group ?: generalGroup,
            message = serializeMessageToJson(Message(message))
        )
    }

    /**
     * Send a keep alive to all active sseEmitters.
     */
    private fun keepAlive() = lock.withLock {
        val keepAlive = SseEvent.KeepAlive(Instant.now().epochSecond)
        sseEmittersToKeepAlive.forEach { sseEmitter ->
            try {
                keepAlive.writeTo(sseEmitter)
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
    }

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        private fun serializeMessageToJson(message: Message) =
            objectMapper.writeValueAsString(message)

        private fun deserializeJsonToMessage(message: String) =
            objectMapper.readValue(message, Message::class.java)
    }
}
