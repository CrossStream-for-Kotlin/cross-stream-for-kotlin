package pt.isel.leic.cs4k.independent

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.Broker
import pt.isel.leic.cs4k.Broker.Companion.SYSTEM_TOPIC
import pt.isel.leic.cs4k.Broker.Companion.UNKNOWN_IDENTIFIER
import pt.isel.leic.cs4k.common.AssociatedSubscribers
import pt.isel.leic.cs4k.common.BaseSubscriber
import pt.isel.leic.cs4k.common.BrokerException
import pt.isel.leic.cs4k.common.BrokerException.BrokerTurnOffException
import pt.isel.leic.cs4k.common.BrokerException.UnexpectedBrokerException
import pt.isel.leic.cs4k.common.BrokerSerializer
import pt.isel.leic.cs4k.common.BrokerThreadType
import pt.isel.leic.cs4k.common.Event
import pt.isel.leic.cs4k.common.RetryExecutor
import pt.isel.leic.cs4k.common.Subscriber
import pt.isel.leic.cs4k.independent.messaging.LineReader
import pt.isel.leic.cs4k.independent.messaging.MessageQueue
import pt.isel.leic.cs4k.independent.network.ConnectionState.CONNECTED
import pt.isel.leic.cs4k.independent.network.ConnectionState.DISCONNECTED
import pt.isel.leic.cs4k.independent.network.ConnectionState.ZOMBIE
import pt.isel.leic.cs4k.independent.network.InboundConnection
import pt.isel.leic.cs4k.independent.network.Neighbors
import pt.isel.leic.cs4k.independent.network.OutboundConnection
import pt.isel.leic.cs4k.independent.network.acceptSuspend
import pt.isel.leic.cs4k.independent.network.connectSuspend
import pt.isel.leic.cs4k.independent.network.readSuspend
import pt.isel.leic.cs4k.independent.network.writeSuspend
import pt.isel.leic.cs4k.independent.serviceDiscovery.DNSServiceDiscovery
import pt.isel.leic.cs4k.independent.serviceDiscovery.MulticastServiceDiscovery
import pt.isel.leic.cs4k.independent.serviceDiscovery.config.DNSServiceDiscoveryConfig
import pt.isel.leic.cs4k.independent.serviceDiscovery.config.MulticastServiceDiscoveryConfig
import pt.isel.leic.cs4k.independent.serviceDiscovery.config.ServiceDiscoveryConfig
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.util.UUID
import kotlin.concurrent.thread
import kotlin.time.Duration

/**
 * Broker Independent.
 *
 * @property hostname The hostname or ip address.
 * @property serviceDiscoveryConfig Service Discovery configuration.
 * @property identifier Identifier of instance/node used in logs.
 * @property enableLogging Logging mode to view logs with system topic [SYSTEM_TOPIC].
 * @property threadBuilder Responsible for creating threads.
 */
class BrokerIndependent(
    private val hostname: String,
    private val serviceDiscoveryConfig: ServiceDiscoveryConfig,
    private val identifier: String = UNKNOWN_IDENTIFIER,
    private val enableLogging: Boolean = false,
    private val threadBuilder: Thread.Builder = Thread.ofVirtual()
) : Broker {

    // Shutdown state.
    private var isShutdown = false

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // The set of neighbors that the broker knows.
    private val neighbors = Neighbors()

    // The node's own IP address.
    private val selfIp = InetAddress.getByName(hostname).hostAddress

    // The listening port.
    private var listeningPort: Int? = null

    // The queue of events to be processed.
    private val controlQueue = MessageQueue<Event>(EVENTS_TO_PROCESS_CAPACITY)

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Thread where coroutines will be created from.
    private val brokerThread: Thread

    // Scope for launch coroutines.
    private var scope: CoroutineScope? = null

    init {
        // Creating a thread from the builder and executing setup.
        brokerThread = threadBuilder.start {
            setup()
        }
    }

    /**
     * Does the initial steps to start up.
     */
    private fun setup() {
        try {
            runBlocking {
                supervisorScope {
                    scope = this
                    val boundServerSocketChannelToAcceptConnectionJob = this.launch {
                        boundServerSocketChannelToAcceptConnection(this)
                    }
                    val periodicConnectToNeighboursJob = this.launch {
                        periodicConnectToNeighbours(this)
                    }
                    val processEventsJob = this.launch {
                        processEvents()
                    }

                    joinAll(
                        boundServerSocketChannelToAcceptConnectionJob,
                        periodicConnectToNeighboursJob,
                        processEventsJob
                    )
                }
            }
        } catch (ex: Exception) {
            if (ex is CancellationException || ex is InterruptedException) {
                return
            } else {
                throw ex
            }
        }
    }

    /**
     * Bound a server socket channel to accept inbound connection.
     * Start a service discovery.
     * For each inbound connection launches a coroutine to listen for messages.
     *
     * @param scope Scope to launch coroutines.
     */
    private suspend fun boundServerSocketChannelToAcceptConnection(scope: CoroutineScope) {
        retryExecutor.suspendExecute({ UnexpectedBrokerException() }, {
            AsynchronousServerSocketChannel.open().use { serverSocketChannel ->
                val port = getPort(serviceDiscoveryConfig)
                listeningPort = port
                val serviceDiscovery = startServiceDiscovery(port)

                try {
                    val inetSocketAddress = InetSocketAddress(selfIp, port)
                    serverSocketChannel.bind(inetSocketAddress)
                    logger.info("[{}:{}] server socket bound", selfIp, port)

                    serviceDiscovery.start()

                    while (true) {
                        val socketChannel = serverSocketChannel.acceptSuspend()
                        scope.launch {
                            processNeighbourInboundConnection(socketChannel)
                        }
                    }
                } catch (ex: Exception) {
                    serviceDiscovery.stop()
                    if (ex is CancellationException || ex is InterruptedException) {
                        logger.info("[{}:{}] server socket closed", selfIp, port)
                        return@suspendExecute
                    } else {
                        throw ex
                    }
                }
            }
        })
    }

    /**
     * Start service discovery.
     *
     * @param port to announce.
     * @return The service discovery instance.
     */
    private fun startServiceDiscovery(port: Int) =
        when (serviceDiscoveryConfig) {
            is DNSServiceDiscoveryConfig ->
                DNSServiceDiscovery(
                    hostname,
                    serviceDiscoveryConfig.serviceName,
                    COMMON_PORT,
                    neighbors,
                    serviceDiscoveryConfig.periodicServiceDiscoveryUpdates
                )
            is MulticastServiceDiscoveryConfig ->
                MulticastServiceDiscovery(
                    neighbors,
                    selfIp,
                    port,
                    serviceDiscoveryConfig.multicastIp,
                    serviceDiscoveryConfig.multicastPort,
                    serviceDiscoveryConfig.periodicServiceDiscoveryUpdates
                )
            else -> throw UnexpectedBrokerException()
        }

    /**
     * Process neighbour inbound connection.
     *
     * @param socketChannel The establish inbound socket channel.
     */
    private suspend fun processNeighbourInboundConnection(socketChannel: AsynchronousSocketChannel) {
        val inetAddress = (socketChannel.remoteAddress as InetSocketAddress).address
        val inboundConnection = InboundConnection(socketChannel)
        logger.info("[{}:{}] <- [{}]", selfIp, listeningPort, inetAddress)
        neighbors.updateInboundConnection(inetAddress, inboundConnection)
        listenSocketChannel(inetAddress, inboundConnection)
    }

    /**
     * Listen for messages in neighbour inbound connection.
     *
     * @param inetAddress The inetAddress that established the connection.
     * @param inboundConnection The inbound connection.
     */
    private suspend fun listenSocketChannel(inetAddress: InetAddress, inboundConnection: InboundConnection) {
        try {
            val lineReader = LineReader { inboundConnection.socketChannel.readSuspend(it) }
            while (true) {
                lineReader.readLine()?.let { line ->
                    logger.info("[{}:{}] <- '{}' <- [{}]", selfIp, listeningPort, line, inetAddress.hostAddress)
                    val event = BrokerSerializer.deserializeEventFromJson(line)
                    deliverToSubscribers(event)
                }
            }
        } catch (ex: Exception) {
            logger.info("[{}:{}] <-X- [{}]", selfIp, listeningPort, inetAddress.hostAddress)
            withContext(Dispatchers.IO) { inboundConnection.socketChannel.close() }
            neighbors.updateInboundConnection(inetAddress, null)
        }
    }

    /**
     * Periodically refreshing the neighbors.
     * In case of having a new neighbor, try to connect to it.
     * If some reason the connection is lost, try to reconnect.
     *
     * @param scope Scope to launch coroutines.
     */
    private suspend fun periodicConnectToNeighbours(scope: CoroutineScope) {
        while (true) {
            try {
                neighbors
                    .getAll()
                    .forEach { neighbor ->
                        if (!neighbor.isOutboundConnectionActive && neighbor.port != null) {
                            val inetSocketAddress = InetSocketAddress(neighbor.inetAddress, neighbor.port)
                            try {
                                val outboundSocketChannel = AsynchronousSocketChannel.open()
                                outboundSocketChannel.connectSuspend(inetSocketAddress)
                                neighbors.update(
                                    neighbor.copy(
                                        outboundConnection = OutboundConnection(
                                            state = CONNECTED,
                                            inetSocketAddress = inetSocketAddress,
                                            socketChannel = outboundSocketChannel
                                        )
                                    )
                                )
                                scope.launch {
                                    writeToOutboundConnection(neighbor.inetAddress, neighbor.port)
                                }

                                logger.info("[{}:{}] -> [{}:{}]", selfIp, listeningPort, neighbor.inetAddress, neighbor.port)
                            } catch (ex: Exception) {
                                logger.info("[{}:{}] -X-> [{}:{}]", selfIp, listeningPort, neighbor.inetAddress, neighbor.port)
                                if (neighbor.outboundConnection?.reachMaximumNumberOfConnectionAttempts == true) {
                                    neighbors.remove(neighbor)
                                } else {
                                    neighbors.update(
                                        neighbor.copy(
                                            outboundConnection = OutboundConnection(
                                                state = ZOMBIE,
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
                            }
                        }
                    }
                delay(DEFAULT_WAIT_TIME_TO_REFRESH_NEIGHBOURS_AGAIN)
            } catch (ex: Exception) {
                logger.info("[{}:{}] stop periodic connect to neighbours", selfIp, listeningPort)
                break
            }
        }
    }

    /**
     * Process the stored events, i.e., deliver the events to the subscribers and store in neighbors events queue.
     */
    private suspend fun processEvents() {
        while (true) {
            val event = controlQueue.dequeue(Duration.INFINITE)
            // Deliver the event to the subscribers.
            deliverToSubscribers(event)
            // Send the event to the neighbors.
            neighbors.getAll().forEach { neighbor ->
                neighbor.eventQueue.enqueue(event)
            }
        }
    }

    /**
     * Deliver event to subscribers, i.e., call all subscriber handlers of the event topic.
     */
    private fun deliverToSubscribers(event: Event) {
        associatedSubscribers
            .getAll(event.topic)
            .forEach { subscriber -> subscriber.handler(event) }
    }

    /**
     * Write to neighbour outbound connection.
     *
     * @param inetAddress The neighbour inetAddress.
     * @param port The neighbour port number.
     */
    private suspend fun writeToOutboundConnection(inetAddress: InetAddress, port: Int) {
        while (true) {
            neighbors.get(inetAddress, port)?.let { neighbor ->
                val event = neighbor.eventQueue.dequeue(Duration.INFINITE)
                if (neighbor.isOutboundConnectionActive) {
                    val outboundConnection = requireNotNull(neighbor.outboundConnection)
                    val socketChannel = requireNotNull(outboundConnection.socketChannel)
                    val eventJson = BrokerSerializer.serializeEventToJson(event)
                    try {
                        socketChannel.writeSuspend(eventJson)
                        logger.info("[{}:{}] -> '{}' -> [{}:{}]", selfIp, listeningPort, eventJson, neighbor.inetAddress, neighbor.port)
                    } catch (ex: Exception) {
                        logger.info("[{}:{}] -> '{}' -X-> [{}:{}]", selfIp, listeningPort, eventJson, neighbor.inetAddress, neighbor.port)
                        withContext(Dispatchers.IO) { socketChannel.close() }
                        neighbors.update(
                            neighbor.copy(
                                outboundConnection = outboundConnection.copy(
                                    state = DISCONNECTED,
                                    socketChannel = null
                                )
                            )
                        )
                        neighbor.eventQueue.enqueue(event)
                    }
                } else {
                    neighbor.eventQueue.enqueue(event)
                }
            } ?: break
        }
    }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown) {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)

        logAndNotifySystemTopic("new subscriber topic '$topic' id '${subscriber.id}'")

        return { unsubscribe(topic, subscriber) }
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown) {
            throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")
        }
        if (topic == SYSTEM_TOPIC) {
            throw BrokerException.UnauthorizedTopicException()
        }

        val event = Event(topic, IGNORE_EVENT_ID, message, isLastMessage)
        runBlocking {
            // Enqueue the event to be processed.
            controlQueue.enqueue(event)
        }
        logAndNotifySystemTopic("publish topic '$topic' event '$event'")
    }

    override fun shutdown() {
        if (isShutdown) {
            throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")
        }

        isShutdown = true
        scope?.cancel()
        neighbors.getAll().forEach {
            it.outboundConnection?.socketChannel?.close()
            it.inboundConnection?.socketChannel?.close()
        }
        brokerThread.interrupt()
        brokerThread.join()
    }

    /**
     * Unsubscribe from a topic.
     *
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     */
    private fun unsubscribe(topic: String, subscriber: BaseSubscriber) {
        associatedSubscribers.removeIf(topic, { sub -> sub.id == subscriber.id })
        logAndNotifySystemTopic("unsubscribe topic '$topic' id '${subscriber.id}'")
    }

    /**
     * Log the message and notify, if logging mode is active, the topic [SYSTEM_TOPIC] with the message.
     *
     * @param message The message to send.
     */
    private fun logAndNotifySystemTopic(message: String) {
        val logMessage = "[$identifier:$selfIp:$listeningPort] $message"
        logger.info(logMessage)
        if (enableLogging) {
            runBlocking {
                controlQueue.enqueue(Event(SYSTEM_TOPIC, IGNORE_EVENT_ID, message, false))
            }
        }
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerIndependent::class.java)

        private const val COMMON_PORT = 6790
        private const val FIRST_AVAILABLE_PORT = 6700
        private const val LAST_AVAILABLE_PORT = 6900
        private const val EVENTS_TO_PROCESS_CAPACITY = 1_000_000
        private const val DEFAULT_WAIT_TIME_TO_REFRESH_NEIGHBOURS_AGAIN = 2000L
        private const val IGNORE_EVENT_ID = -1L

        /**
         * Get a port number to bound server socket channel.
         *
         * @param serviceDiscoveryConfig The service discovery configuration.
         */
        private fun getPort(serviceDiscoveryConfig: ServiceDiscoveryConfig) =
            if (serviceDiscoveryConfig is MulticastServiceDiscoveryConfig) {
                (FIRST_AVAILABLE_PORT..LAST_AVAILABLE_PORT).random()
            } else {
                COMMON_PORT
            }
    }
}
