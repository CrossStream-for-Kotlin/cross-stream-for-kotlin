package pt.isel.leic.cs4k.independent.network

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.independent.messaging.LineReader
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.assertEquals

class AsynchronousSocketChannelTests {
    private lateinit var serverSocket: AsynchronousServerSocketChannel

    @BeforeEach
    fun setup() {
        Thread { runServer() }.start()
        Thread.sleep(1000)
    }

    @AfterEach
    fun tearDown() {
        logger.info("Closing server socket")
        serverSocket.close()
    }

    @Test
    fun `connect to server`() {
        runBlocking {
            val socketChannel = AsynchronousSocketChannel.open()
            socketChannel.connectSuspend(SERVER_ADDRESS)
            logger.info("Connection to server established")
            assertTrue(socketChannel.isOpen)
            socketChannel.close()
        }
    }

    @Test
    fun `fail to connect to invalid address`() {
        runBlocking {
            val socketChannel = AsynchronousSocketChannel.open()
            val exception = assertThrows<Exception> {
                runBlocking {
                    socketChannel.connectSuspend(InetSocketAddress("0.0.0.0", 8888))
                }
            }
            logger.info("Failed to connect as expected: {}", exception.message)
            assertFalse(socketChannel.isOpen)
        }
    }

    @Test
    fun `read from server after connection`() {
        runBlocking {
            val socketChannel = AsynchronousSocketChannel.open()
            socketChannel.connectSuspend(SERVER_ADDRESS)
            val byteBuffer = ByteBuffer.allocate(1024)
            val bytesRead = socketChannel.readSuspend(byteBuffer)
            assertTrue(bytesRead > 0)
            val response = String(byteBuffer.array(), 0, bytesRead)
            assertEquals("Welcome client\n", response)
            socketChannel.close()
        }
    }

    @Test
    fun `write to server and received the same message`() {
        runBlocking {
            val socketChannel = AsynchronousSocketChannel.open()
            socketChannel.connectSuspend(SERVER_ADDRESS)

            val welcomeBuffer = ByteBuffer.allocate(1024)
            val welcomeBytesRead = socketChannel.readSuspend(welcomeBuffer)
            assertTrue(welcomeBytesRead > 0)
            val welcomeMessage = String(welcomeBuffer.array(), 0, welcomeBytesRead)
            assertEquals("Welcome client\n", welcomeMessage)

            val success = socketChannel.writeSuspend("Hello, server")
            assertTrue(success)

            val echoBuffer = ByteBuffer.allocate(1024)
            val echoBytesRead = socketChannel.readSuspend(echoBuffer)
            assertTrue(echoBytesRead > 0)
            val echoMessage = String(echoBuffer.array(), 0, echoBytesRead)
            assertEquals("Hello, server\n", echoMessage)

            socketChannel.close()
        }
    }

    @Test
    fun `connect, read and send to server`() {
        runBlocking {
            // connect
            val socketChannel = AsynchronousSocketChannel.open()
            socketChannel.connectSuspend(SERVER_ADDRESS)
            logger.info("connect completed")

            // read
            val lineReader = LineReader { socketChannel.readSuspend(it) }
            val line = lineReader.readLine()
            assertEquals(line, "Welcome client")
            logger.info("read completed")

            // write
            val success = socketChannel.writeSuspend("test")
            assertTrue(success)
            logger.info("write completed")
        }
    }

    @Test
    fun `attempt to read from closed socket`() {
        runBlocking {
            val socketChannel = AsynchronousSocketChannel.open()
            socketChannel.connectSuspend(SERVER_ADDRESS)
            socketChannel.close()
            val byteBuffer = ByteBuffer.allocate(1024)
            val exception = assertThrows<Exception> {
                runBlocking {
                    socketChannel.readSuspend(byteBuffer)
                }
            }
            logger.info("Read attempt from closed socket failed as expected: {}", exception.message)
        }
    }

    @Test
    fun `accept multiple connections successfully`() {
        runBlocking {
            val clientSocket1 = AsynchronousSocketChannel.open()
            val clientSocket2 = AsynchronousSocketChannel.open()

            clientSocket1.connectSuspend(SERVER_ADDRESS)

            clientSocket2.connectSuspend(SERVER_ADDRESS)

            assertTrue(clientSocket1.isOpen)
            assertTrue(clientSocket2.isOpen)

            clientSocket1.close()
            clientSocket2.close()
        }
    }

    @Test
    fun `stress test accepting multiple connections`() = runBlocking {
        val clientSockets = mutableListOf<Job>()
        val results = ConcurrentLinkedQueue<Boolean>()

        repeat(NUMBER_CONNECTIONS) {
            val clientJob = launch(Dispatchers.IO) {
                val clientSocket = AsynchronousSocketChannel.open()
                val result = try {
                    clientSocket.connectSuspend(SERVER_ADDRESS)
                    clientSocket.isOpen
                } catch (e: Exception) {
                    false
                } finally {
                    clientSocket.close()
                }
                results.add(result)
            }
            clientSockets.add(clientJob)
        }

        clientSockets.joinAll()

        assertTrue(results.all { it }, "Not all connections were successful")

        assertTrue(serverSocket.isOpen, "Server socket must be open")
    }

    @Test
    fun `fail to accept connection`() {
        runBlocking {
            val clientSocket = AsynchronousSocketChannel.open()

            val exception = assertThrows<Exception> {
                runBlocking {
                    serverSocket.close()
                    clientSocket.connectSuspend(SERVER_ADDRESS)
                }
            }

            assertTrue(exception is Exception)
            clientSocket.close()
        }
    }

    private fun runServer() {
        runBlocking {
            serverSocket = AsynchronousServerSocketChannel.open()
            serverSocket.bind(InetSocketAddress(8888))
            logger.info("server started listening")
            while (true) {
                try {
                    val clientSocket = serverSocket.acceptSuspend()
                    launch {
                        handleClient(clientSocket)
                    }
                } catch (e: Exception) {
                    logger.error("Error accepting connection: ${e.message}")
                }
            }
        }
    }

    private suspend fun handleClient(clientSocket: AsynchronousSocketChannel) {
        try {
            val lineReader = LineReader { clientSocket.readSuspend(it) }
            val success = clientSocket.writeSuspend("Welcome client")
            logger.info("State of writing = {}", success)
            val content = lineReader.readLine()
            if (content != null) {
                clientSocket.writeSuspend(content)
                logger.info("Message received: {}", content)
            }
        } catch (e: Exception) {
            logger.error("Error handling client: ${e.message}")
        } finally {
            clientSocket.close()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AsynchronousSocketChannelTests::class.java)
        const val NUMBER_CONNECTIONS = 100
        private val SERVER_ADDRESS = InetSocketAddress("127.0.0.1", 8888)
    }
}
