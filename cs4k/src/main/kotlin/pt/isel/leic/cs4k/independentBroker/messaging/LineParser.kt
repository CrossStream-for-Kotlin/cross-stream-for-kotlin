package pt.isel.leic.cs4k.independentBroker.messaging

import java.nio.CharBuffer
import java.util.LinkedList

/**
 * Receives CharBuffers and provides Strings, partitioned by line breaks.
 */
class LineParser {

    // Holds the line being parsed.
    private val stringBuilder = StringBuilder()

    // Holds the already parsed lines.
    private val lines = LinkedList<String>()

    // The previous char, if it is a terminator.
    private var lastTerminator: Char? = null

    fun offer(chars: CharBuffer) {
        while (chars.position() != chars.limit()) {
            offer(chars.get())
        }
    }

    fun poll(): String? = if (lines.isNotEmpty()) {
        lines.poll()
    } else {
        null
    }

    private fun offer(char: Char) {
        lastTerminator = if (isTerminator(char)) {
            if (lastTerminator == null || lastTerminator == char) {
                // Assume that this terminator is ending a line.
                extractLine()
                char
            } else {
                // Merge this with last terminator.
                null
            }
        } else {
            stringBuilder.append(char)
            null
        }
    }

    private fun extractLine() {
        lines.add(stringBuilder.toString())
        stringBuilder.clear()
    }

    private companion object {
        private fun isTerminator(c: Char) = c == '\n' || c == '\r'
    }
}
