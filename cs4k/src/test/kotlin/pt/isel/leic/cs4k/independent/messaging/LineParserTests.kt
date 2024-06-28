package pt.isel.leic.cs4k.independent.messaging

import org.junit.jupiter.api.Test
import java.nio.CharBuffer
import kotlin.test.assertEquals
import kotlin.test.assertNull

class LineParserTests {

    @Test
    fun `check line parser`() {
        val lineParser = LineParser()

        lineParser.offer("hel")
        assertNull(lineParser.poll())

        lineParser.offer("lo\n")
        assertEquals("hello", lineParser.poll())

        lineParser.offer("\r")
        assertNull(lineParser.poll())

        lineParser.offer("\r")
        assertEquals("", lineParser.poll())

        lineParser.offer("\r")
        assertEquals("", lineParser.poll())

        lineParser.offer("\n")
        assertNull(lineParser.poll())
    }

    companion object {
        fun LineParser.offer(s: String) = this.offer(CharBuffer.wrap(s))
    }
}
