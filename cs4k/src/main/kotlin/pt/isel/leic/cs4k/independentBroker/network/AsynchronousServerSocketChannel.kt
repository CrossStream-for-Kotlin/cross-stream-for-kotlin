package pt.isel.leic.cs4k.independentBroker.network

import kotlinx.coroutines.suspendCancellableCoroutine
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Suspend function that accepts inbound connections, build on top of [AsynchronousServerSocketChannel].
 *
 * @return The [AsynchronousSocketChannel] of the established inbound connection.
 */
suspend fun AsynchronousServerSocketChannel.acceptSuspend(): AsynchronousSocketChannel {
    return suspendCancellableCoroutine { continuation ->
        this.accept(
            null,
            object : CompletionHandler<AsynchronousSocketChannel?, Unit?> {
                override fun completed(result: AsynchronousSocketChannel?, attachment: Unit?) {
                    requireNotNull(result) { "The 'result' should not be null." }
                    continuation.resume(result)
                }

                override fun failed(exc: Throwable?, attachment: Unit?) {
                    requireNotNull(exc) { "The 'exc' should not be null." }
                    continuation.resumeWithException(exc)
                }
            }
        )
    }
}
