package pt.isel.leic.cs4k.postgreSQL

/**
 * Represents operations commands on a channel.
 */
sealed class ChannelCommandOperation {

    /**
     * Represents the operation of listening on a channel.
     */
    object Listen : ChannelCommandOperation() {
        override fun toString() = LISTEN
    }

    /**
     * Represents the operation of stopping listening on a channel.
     */
    object UnListen : ChannelCommandOperation() {
        override fun toString() = UNLISTEN
    }

    private companion object {
        // Listen command.
        private const val LISTEN = "listen"

        // UnListen command.
        private const val UNLISTEN = "unlisten"
    }
}
