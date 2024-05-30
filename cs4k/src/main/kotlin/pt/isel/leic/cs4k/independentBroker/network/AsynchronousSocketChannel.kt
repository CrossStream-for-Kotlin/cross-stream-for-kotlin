package pt.isel.leic.cs4k.independentBroker.network

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Suspend function that establish outbound connections, build on top of [AsynchronousSocketChannel].
 * If coroutine is cancelled, closes socket.
 *
 * @param socketAddress The socket address to connect to.
 */
suspend fun AsynchronousSocketChannel.connectSuspend(socketAddress: InetSocketAddress) {
    return try {
        suspendCancellableCoroutine { continuation ->
            this.connect(
                socketAddress,
                null,
                object : CompletionHandler<Void?, Unit?> {
                    override fun completed(result: Void?, attachment: Unit?) {
                        continuation.resume(Unit)
                    }

                    override fun failed(exc: Throwable?, attachment: Unit?) {
                        requireNotNull(exc) { "The 'exc' should not be null." }
                        continuation.resumeWithException(exc)
                    }
                }
            )
        }
    } catch (e: CancellationException) {
        withContext(Dispatchers.IO) {
            this@connectSuspend.close()
        }
        throw e
    }
}

/**
 * Suspend function that reads messages from inbound connections, build on top of [AsynchronousSocketChannel].
 *
 * @param byteBuffer The ByteBuffer to store the content of the received message.
 * @return The length of the received message.
 */
suspend fun AsynchronousSocketChannel.readSuspend(byteBuffer: ByteBuffer): Int {
    val readLength = suspendCancellableCoroutine { continuation ->
        this.read(
            byteBuffer,
            null,
            object : CompletionHandler<Int?, Unit?> {
                override fun completed(result: Int?, attachment: Unit?) {
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
    return readLength
}

/**
 * Suspend function that writes messages to outbound connections, build on top of [AsynchronousSocketChannel].
 *
 * @param text The text to write.
 * @return Returns true if the text written matches what was intended to be written.
 */
suspend fun AsynchronousSocketChannel.writeSuspend(text: String): Boolean {
    val textWithTerminationCharacter = if (text.last() == '\n' || text.last() == '\r') text else text + "\n"
    val byteBuffer = withContext(Dispatchers.IO) {
        Charsets.UTF_8.newEncoder().encode(CharBuffer.wrap(textWithTerminationCharacter))
    }.also {
        it.mark()
    }
    val writeLength = suspendCancellableCoroutine { continuation ->
        this.write(
            byteBuffer,
            null,
            object : CompletionHandler<Int?, Unit?> {
                override fun completed(result: Int?, attachment: Unit?) {
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
    return String(byteBuffer.reset().array(), 0, writeLength) == textWithTerminationCharacter
}
