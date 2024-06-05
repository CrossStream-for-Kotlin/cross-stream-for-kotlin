package pt.isel.leic.cs4k.demo.http

import jakarta.servlet.http.HttpServletRequest
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import pt.isel.leic.cs4k.demo.http.models.input.MessageInputModel
import pt.isel.leic.cs4k.demo.services.ChatService

@RestController
class ChatController(
    private val chatService: ChatService
) {

    @GetMapping(Uris.Chat.LISTEN)
    fun listen(request: HttpServletRequest, @RequestParam group: String?): SseEmitter =
        chatService.newListener(
            group = group,
            subscribedNode = request.getAttribute("node").toString()
        )

    @PostMapping(Uris.Chat.SEND)
    fun send(request: HttpServletRequest, @RequestParam group: String?, @RequestBody body: MessageInputModel) {
        chatService.sendMessage(
            group = group,
            message = body.message,
            publishingNode = request.getAttribute("node").toString()
        )
    }
}
