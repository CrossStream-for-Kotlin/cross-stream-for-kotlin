package pt.isel.leic.cs4k.demo.services

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

sealed interface SseEvent {

    fun writeTo(emitter: SseEmitter)

    class Message(name: String, id: Long, data: Any) : SseEvent {

        private val event = SseEmitter.event()
            .name(name)
            .id(id.toString())
            .data(data)

        override fun writeTo(emitter: SseEmitter) {
            emitter.send(event)
        }
    }

    class KeepAlive(timestamp: Long) : SseEvent {

        private val event = SseEmitter.event()
            .comment(timestamp.toString())

        override fun writeTo(emitter: SseEmitter) {
            emitter.send(event)
        }
    }
}
