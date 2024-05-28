package pt.isel.leic.cs4k.demo.http

object Uris {

    const val PREFIX = "/api"

    object Chat {
        const val LISTEN = "$PREFIX/chat/listen"
        const val SEND = "$PREFIX/chat/send"
    }
}
