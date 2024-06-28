package pt.isel.leic.cs4k.independent.network

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import java.net.InetAddress

class NeighborsTests {

    private lateinit var neighbors: Neighbors

    @BeforeEach
    fun setUp() {
        neighbors = Neighbors()
    }

    @Test
    fun `test add and get all neighbors`() {
        val neighbor1 = Neighbor(InetAddress.getByName("192.168.0.1"))
        val neighbor2 = Neighbor(InetAddress.getByName("192.168.0.2"))

        neighbors.add(neighbor1)
        neighbors.add(neighbor2)

        val allNeighbors = neighbors.getAll()
        assertEquals(2, allNeighbors.size)
        assertTrue(allNeighbors.contains(neighbor1))
        assertTrue(allNeighbors.contains(neighbor2))
    }

    @Test
    fun `test add duplicate neighbor`() {
        val neighbor = Neighbor(InetAddress.getByName("192.168.0.1"))

        neighbors.add(neighbor)
        neighbors.add(neighbor)

        val allNeighbors = neighbors.getAll()
        assertEquals(1, allNeighbors.size)
    }

    @Test
    fun `test addAll neighbors`() {
        val neighbor1 = Neighbor(InetAddress.getByName("192.168.0.1"))
        val neighbor2 = Neighbor(InetAddress.getByName("192.168.0.2"))
        val neighbor3 = Neighbor(InetAddress.getByName("192.168.0.3"))

        neighbors.addAll(setOf(neighbor1, neighbor2))
        neighbors.addAll(setOf(neighbor2, neighbor3))

        val allNeighbors = neighbors.getAll()
        assertEquals(3, allNeighbors.size)
        assertTrue(allNeighbors.contains(neighbor1))
        assertTrue(allNeighbors.contains(neighbor2))
        assertTrue(allNeighbors.contains(neighbor3))
    }

    @Test
    fun `test remove neighbor`() {
        val neighbor = Neighbor(InetAddress.getByName("192.168.0.1"))

        neighbors.add(neighbor)
        neighbors.remove(neighbor)

        val allNeighbors = neighbors.getAll()
        assertTrue(allNeighbors.isEmpty())
    }

    @Test
    fun `test updateAndGet existing neighbor`() {
        val address = InetAddress.getByName("192.168.0.1")
        val inboundConnection1 = mock(InboundConnection::class.java)
        val inboundConnection2 = mock(InboundConnection::class.java)

        val neighbor = Neighbor(address, inboundConnection1)
        neighbors.add(neighbor)

        val updatedNeighbor = neighbors.updateAndGet(address, inboundConnection2)

        assertEquals(inboundConnection2, updatedNeighbor.inboundConnection)
        assertEquals(1, neighbors.getAll().size)
        assertTrue(neighbors.getAll().contains(updatedNeighbor))
    }

    @Test
    fun `test updateAndGet new neighbor`() {
        val address = InetAddress.getByName("192.168.0.1")
        val inboundConnection = mock(InboundConnection::class.java)

        val updatedNeighbor = neighbors.updateAndGet(address, inboundConnection)

        assertEquals(inboundConnection, updatedNeighbor.inboundConnection)
        assertEquals(1, neighbors.getAll().size)
        assertTrue(neighbors.getAll().contains(updatedNeighbor))
    }

    @Test
    fun `test update neighbor`() {
        val address = InetAddress.getByName("192.168.0.1")
        val inboundConnection1 = mock(InboundConnection::class.java)
        val inboundConnection2 = mock(InboundConnection::class.java)

        val neighbor = Neighbor(address, inboundConnection1)
        neighbors.add(neighbor)

        val updatedNeighbor = neighbor.copy(inboundConnection = inboundConnection2)
        neighbors.update(updatedNeighbor)

        val allNeighbors = neighbors.getAll()
        assertEquals(1, allNeighbors.size)
        assertTrue(allNeighbors.contains(updatedNeighbor))
        assertEquals(inboundConnection2, allNeighbors.first().inboundConnection)
    }

    @Test
    fun `stress test adding multiple neighbors`() {
        runBlocking {
            val jobs = mutableListOf<Job>()

            repeat(NUMBER_COROUTINES) { coroutineIndex ->
                jobs.add(
                    launch(Dispatchers.Default) {
                        repeat(NUMBER_NEIGHBORS) { _ ->
                            val address = InetAddress.getByName("192.168.0.${coroutineIndex % 255}")
                            val neighbor = Neighbor(address, inboundConnection = mock(InboundConnection::class.java))
                            neighbors.add(neighbor)
                        }
                    }
                )
            }

            jobs.joinAll()

            val allNeighbors = neighbors.getAll()
            allNeighbors.forEach { neighbor ->
                assertNotNull(neighbor.inetAddress)
                assertNotNull(neighbor.inboundConnection)
            }

            val distinctNeighbors = allNeighbors.distinctBy { it.inetAddress }
            assertEquals(allNeighbors.size, distinctNeighbors.size)
        }
    }

    @Test
    fun `stress test adding multiple neighbors and then update or remove randomly`() = runBlocking {
        val addJobs = mutableListOf<Job>()
        val updateRemoveJobs = mutableListOf<Job>()

        val ipAddresses = (0 until NUMBER_NEIGHBORS).map { i ->
            InetAddress.getByName("192.168.${i / 255}.${i % 255}")
        }

        repeat(NUMBER_COROUTINES) { coroutineIndex ->
            addJobs.add(
                launch(Dispatchers.Default) {
                    val start = coroutineIndex * (NUMBER_NEIGHBORS / NUMBER_COROUTINES)
                    val end = start + (NUMBER_NEIGHBORS / NUMBER_COROUTINES)
                    for (i in start until end) {
                        val address = ipAddresses[i]
                        val neighbor = Neighbor(address, inboundConnection = mock(InboundConnection::class.java))
                        neighbors.add(neighbor)
                    }
                }
            )
        }

        addJobs.joinAll()

        val allNeighbors = neighbors.getAll()
        assertEquals(NUMBER_NEIGHBORS, allNeighbors.size)

        repeat(NUMBER_COROUTINES) { coroutineIndex ->
            updateRemoveJobs.add(
                launch(Dispatchers.Default) {
                    val start = coroutineIndex * (NUMBER_NEIGHBORS / NUMBER_COROUTINES)
                    val end = start + (NUMBER_NEIGHBORS / (2 * NUMBER_COROUTINES))
                    for (i in start until end) {
                        val address = ipAddresses[i]
                        if (i % 2 == 0) {
                            neighbors.updateAndGet(address, mock(InboundConnection::class.java))
                        } else {
                            val neighbor = Neighbor(address, inboundConnection = mock(InboundConnection::class.java))
                            neighbors.remove(neighbor)
                        }
                    }
                }
            )
        }

        updateRemoveJobs.joinAll()

        val finalNeighbors = neighbors.getAll()
        val distinctNeighbors = finalNeighbors.distinctBy { it.inetAddress }
        assertEquals(finalNeighbors.size, distinctNeighbors.size)
    }

    companion object {
        private const val NUMBER_COROUTINES = 100
        private const val NUMBER_NEIGHBORS = 1000
    }
}
