package pt.isel.leic.cs4k.independentBroker

import cs4k.prototype.broker.option3.Neighbor
import cs4k.prototype.broker.option3.serviceDiscovery.DNSServiceDiscovery
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.Broker
import pt.isel.leic.cs4k.common.AssociatedSubscribers
import pt.isel.leic.cs4k.common.BrokerException
import pt.isel.leic.cs4k.common.BrokerSerializer
import pt.isel.leic.cs4k.common.Event
import pt.isel.leic.cs4k.common.RetryExecutor
import pt.isel.leic.cs4k.common.Subscriber
import pt.isel.leic.cs4k.independentBroker.messaging.LineReader
import pt.isel.leic.cs4k.independentBroker.messaging.MessageQueue
import pt.isel.leic.cs4k.independentBroker.network.ConnectionState
import pt.isel.leic.cs4k.independentBroker.network.InboundConnection
import pt.isel.leic.cs4k.independentBroker.network.Neighbors
import pt.isel.leic.cs4k.independentBroker.network.OutboundConnection
import pt.isel.leic.cs4k.independentBroker.network.acceptSuspend
import pt.isel.leic.cs4k.independentBroker.network.connectSuspend
import pt.isel.leic.cs4k.independentBroker.network.readSuspend
import pt.isel.leic.cs4k.independentBroker.network.writeSuspend
import java.net.ConnectException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketException
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.util.*
import java.util.concurrent.Executors
import kotlin.concurrent.thread
import kotlin.time.Duration

class BrokerOption3(
    hostname: String,
    serviceName: String,
    private val selfPort: Int = 6790
) : Broker {

    // Shutdown state.
    private var isShutdown = false

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // The set of neighbors that the broker knows.
    private val neighbors = Neighbors()

    // The node's own IP address.
    private val selfIp = InetAddress.getByName(hostname)

    // Service discovery configuration.
    private val serviceDiscovery = /* MulticastServiceDiscovery(
        neighbors,
        hostname,
        selfPort,
        networkInterface = networkInterface
    )*/ DNSServiceDiscovery(serviceName, neighbors, hostname)

    // The queue of events to be processed.
    private val eventsToProcess = MessageQueue<Event>(EVENTS_TO_PROCESS_CAPACITY)

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Scope for launch coroutines.
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    init {
        thread {
            runBlocking {
                supervisorScope {
                    val boundServerSocketChannelToAcceptConnectionJob = this.launch(readCoroutineDispatcher) {
                        boundServerSocketChannelToAcceptConnection(this)
                    }
                    val periodicConnectToNeighboursJob = this.launch(writeCoroutineDispatcher) {
                        periodicConnectToNeighbours()
                    }
                    val processEventsJob = this.launch(writeCoroutineDispatcher) {
                        processEvents()
                    }

                    joinAll(
                        boundServerSocketChannelToAcceptConnectionJob,
                        periodicConnectToNeighboursJob,
                        processEventsJob
                    )
                }
            }
        }
    }

    /**
     * Bound a server socket channel to accept inbound connection.
     * For each inbound connection launches a coroutine to listen for messages.
     *
     * @param scope Scope to launch coroutines.
     */
    private suspend fun boundServerSocketChannelToAcceptConnection(scope: CoroutineScope) {
        retryExecutor.suspendExecute({ BrokerException.UnexpectedBrokerException() }, {
            AsynchronousServerSocketChannel.open().use { serverSocketChannel ->
                serverSocketChannel.bind(InetSocketAddress(selfIp, selfPort))
                logger.info("[{}] server socket bound", selfIp)

                while (true) {
                    val socketChannel = serverSocketChannel.acceptSuspend()
                    scope.launch(readCoroutineDispatcher) {
                        processNeighbourInboundConnection(socketChannel)
                    }
                }
            }
        })
    }

    /**
     * Process neighbour inbound connection.
     *
     * @param socketChannel The establish inbound socket channel.
     */
    private suspend fun processNeighbourInboundConnection(socketChannel: AsynchronousSocketChannel) {
        val inetAddress = (socketChannel.remoteAddress as InetSocketAddress).address
        if (inetAddress != InetAddress.getByName("127.0.0.1")) {
            val inboundConnection = InboundConnection(socketChannel)
            logger.info("[{}] <- [{}]", selfIp, inetAddress)
            val neighbor = neighbors.updateAndGet(inetAddress, inboundConnection)
            listenSocketChannel(neighbor)
        }
        logger.info("The connection is from localhost")
    }

    /**
     * Listen for messages in neighbour inbound connection.
     *
     * @param neighbor The neighbor who established the connection.
     */
    private suspend fun listenSocketChannel(neighbor: Neighbor) {
        while (true) {
            // If the inbound connection is active ...
            if (neighbor.isInboundConnectionActive) {
                val inboundConnection = requireNotNull(neighbor.inboundConnection)
                val socketChannel = requireNotNull(inboundConnection.socketChannel)
                try {
                    // ... read the socket channel.
                    val lineReader = LineReader { socketChannel.readSuspend(it) }
                    lineReader.readLine()?.let { line ->
                        logger.info("[{}] <- '{}' <- [{}]", selfIp, line, neighbor.inetAddress)
                        val event = BrokerSerializer.deserializeEventFromJson(line)
                        deliverToSubscribers(event)
                    }
                } catch (ex: Exception) {
                    neighbors.update(neighbor.copy(inboundConnection = null))
                    logger.info("[{}] <-X- [{}]", selfIp, neighbor.inetAddress)
                    break
                }
            }
        }
    }

    /**
     * Periodically refreshing the neighbors.
     * In case of having a new neighbor, try to connect to it.
     * If some reason the connection is lost, try to reconnect.
     */
    private suspend fun periodicConnectToNeighbours() {
        while (true) {
            neighbors
                .getAll()
                .forEach { neighbor ->
                    if (!neighbor.isOutboundConnectionActive && neighbor.inetAddress != InetAddress.getByName("127.0.0.1")) {
                        val inetSocketAddress = InetSocketAddress(neighbor.inetAddress, selfPort)
                        try {
                            val outboundSocketChannel = AsynchronousSocketChannel.open()
                            outboundSocketChannel.connectSuspend(inetSocketAddress)
                            neighbors.update(
                                neighbor.copy(
                                    outboundConnection = OutboundConnection(
                                        state = ConnectionState.CONNECTED,
                                        inetSocketAddress = inetSocketAddress,
                                        socketChannel = outboundSocketChannel
                                    )
                                )
                            )
                            logger.info("[{}] -> [{}]", selfIp, neighbor.inetAddress)
                        } catch (ex: Exception) {
                            if (ex is ConnectException || ex is SocketException) {
                                if (neighbor.outboundConnection?.reachMaximumNumberOfConnectionAttempts == true) {
                                    neighbors.remove(neighbor)
                                } else {
                                    neighbors.update(
                                        neighbor.copy(
                                            outboundConnection = OutboundConnection(
                                                state = ConnectionState.ZOMBIE,
                                                inetSocketAddress = inetSocketAddress,
                                                socketChannel = null,
                                                numberOfConnectionAttempts = neighbor.outboundConnection
                                                    ?.numberOfConnectionAttempts
                                                    ?.plus(1)
                                                    ?: 1
                                            )
                                        )
                                    )
                                }
                                logger.info("[{}] -X-> [{}]", selfIp, neighbor.inetAddress)
                            } else {
                                throw ex
                            }
                        }
                    }
                }
            delay(DEFAULT_WAIT_TIME_TO_REFRESH_NEIGHBOURS_AGAIN)
        }
    }

    /**
     * Process the stored events, i.e., deliver the events to the subscribers and send to neighbors.
     */
    private suspend fun processEvents() {
        while (true) {
            val event = eventsToProcess.dequeue(Duration.INFINITE)
            // Deliver the event to the subscribers.
            deliverToSubscribers(event)
            // Send the event to the neighbors.
            sendToNeighbors(event)
        }
    }

    /**
     * Deliver event to the subscribers.
     *
     * @param event The event that will be delivered.
     */
    private fun deliverToSubscribers(event: Event) {
        associatedSubscribers
            .getAll(event.topic)
            .forEach { subscriber -> subscriber.handler(event) }
    }

    /**
     * Send the event to the neighbors.
     *
     * @param event The event to send.
     */
    private suspend fun sendToNeighbors(event: Event) {
        neighbors
            .getAll()
            .forEach { neighbor ->
                // If the outbound connection is active ...
                if (neighbor.isOutboundConnectionActive) {
                    val outboundConnection = requireNotNull(neighbor.outboundConnection)
                    val socketChannel = requireNotNull(outboundConnection.socketChannel)
                    val eventJson = BrokerSerializer.serializeEventToJson(event)
                    try {
                        // ... try to write the event to the socket channel.
                        socketChannel.writeSuspend(eventJson)
                        logger.info("[{}] -> '{}' -> [{}]", selfIp, eventJson, neighbor.inetAddress)
                    } catch (ex: SocketException) {
                        neighbors.update(
                            neighbor.copy(
                                outboundConnection = outboundConnection.copy(
                                    state = ConnectionState.DISCONNECTED,
                                    socketChannel = null
                                )
                            )
                        )
                        logger.info("[{}] -> '{}' -X-> [{}]", selfIp, eventJson, neighbor.inetAddress)
                    }
                }
            }
    }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown) throw BrokerException.BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)

        logger.info("[{}] new subscriber topic '{}' id '{}'", selfIp, topic, subscriber.id)

        return { unsubscribe(topic, subscriber) }
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown) throw BrokerException.BrokerTurnOffException("Cannot invoke ${::publish.name}.")

        val event = Event(topic, IGNORE_EVENT_ID, message, isLastMessage)
        runBlocking {
            // Enqueue the event to be processed.
            eventsToProcess.enqueue(event)
        }

        logger.info("[{}] publish topic '{}' event '{}'", selfIp, topic, event)
    }

    override fun shutdown() {
        if (isShutdown) throw BrokerException.BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")

        isShutdown = true
        serviceDiscovery.stop()
        scope.cancel()
    }

    /**
     * Unsubscribe from a topic.
     *
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic, { sub -> sub.id == subscriber.id })
        logger.info("[{}] unsubscribe topic '{}' id '{}'", selfIp, topic, subscriber.id)
    }

    fun toTest() {
        logger.info("The neighbiors of ip {} are {}", selfIp, neighbors.getAll().forEach { it.inetAddress })
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerOption3::class.java)

        // Coroutine dispatcher for read operations.
        private val readCoroutineDispatcher =
            Executors.newFixedThreadPool(2).asCoroutineDispatcher()

        // Coroutine dispatcher for write operations.
        private val writeCoroutineDispatcher =
            Executors.newFixedThreadPool(2).asCoroutineDispatcher()

        // private const val COMMON_PORT = 6790
        private const val EVENTS_TO_PROCESS_CAPACITY = 5000
        private const val DEFAULT_WAIT_TIME_TO_REFRESH_NEIGHBOURS_AGAIN = 2000L
        private const val IGNORE_EVENT_ID = -1L
    }
}
