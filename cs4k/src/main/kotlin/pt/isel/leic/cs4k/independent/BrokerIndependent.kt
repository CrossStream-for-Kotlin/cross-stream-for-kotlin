package pt.isel.leic.cs4k.independent

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.Broker
import pt.isel.leic.cs4k.Broker.Companion.SYSTEM_TOPIC
import pt.isel.leic.cs4k.Broker.Companion.UNKNOWN_IDENTIFIER
import pt.isel.leic.cs4k.common.AssociatedSubscribers
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
import pt.isel.leic.cs4k.independent.network.Neighbor
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

/**
 * Broker Independent.
 *
 * @property hostname The hostname.
 * @property serviceDiscoveryConfig Service Discovery configuration.
 * @property identifier Identifier of instance/node used in logs.
 * @property enableLogging Logging mode to view logs with system topic [SYSTEM_TOPIC].
 * @param brokerThreadType The type of thread used for listening for events and connecting
 * to neighbors.
 */
class BrokerIndependent(
    private val COMMON_PORT: Int,
    private val hostname: String,
    private val serviceDiscoveryConfig: ServiceDiscoveryConfig,
    private val identifier: String = UNKNOWN_IDENTIFIER,
    private val enableLogging: Boolean = false,
    brokerThreadType: BrokerThreadType = BrokerThreadType.VIRTUAL
) : Broker {

    // Shutdown state.
    private var isShutdown = false

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // The set of neighbors that the broker knows.
    private val neighbors = Neighbors()

    // The node's own IP address.
    private val selfIp = InetAddress.getByName(hostname).hostAddress

    // The queue of events to be processed.
    private val controlQueue = MessageQueue<Event>(EVENTS_TO_PROCESS_CAPACITY)

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Thread where coroutines will be created from.
    private val brokerThread: Thread

    // Scope for launch coroutines.
    private lateinit var scope: CoroutineScope

    init {
        brokerThread = if (brokerThreadType == BrokerThreadType.REGULAR) {
            thread {
                setup()
            }
        } else {
            Thread.startVirtualThread {
                setup()
            }
        }
    }

    /**
     * Does the initial steps to start up.
     */
    private fun setup() {
        runBlocking {
            supervisorScope {
                scope = this
                val boundServerSocketChannelToAcceptConnectionJob = this.launch(readCoroutineDispatcher) {
                    boundServerSocketChannelToAcceptConnection(this)
                }
                val periodicConnectToNeighboursJob = this.launch(writeCoroutineDispatcher) {
                    periodicConnectToNeighbours(this)
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
                val inetSocketAddress = InetSocketAddress(selfIp, COMMON_PORT)
                serverSocketChannel.bind(inetSocketAddress)
                logger.info("[{}] server socket bound", selfIp)

                startServiceDiscovery()

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
     * Start service discovery.
     */
    private fun startServiceDiscovery() {
        when (serviceDiscoveryConfig) {
            is DNSServiceDiscoveryConfig ->
                DNSServiceDiscovery(serviceDiscoveryConfig.hostname, serviceDiscoveryConfig.serviceName, neighbors)
                    .start()

            is MulticastServiceDiscoveryConfig ->
                MulticastServiceDiscovery(
                    neighbors,
                    InetAddress.getByName(serviceDiscoveryConfig.hostname).hostAddress,
                    selfIp
                )
                    .start()
        }
    }

    /**
     * Process neighbour inbound connection.
     *
     * @param socketChannel The establish inbound socket channel.
     */


    /**
     * Process neighbour inbound connection.
     *
     * @param socketChannel The establish inbound socket channel.
     */
    private suspend fun processNeighbourInboundConnection(socketChannel: AsynchronousSocketChannel) {
        val inetAddress = (socketChannel.remoteAddress as InetSocketAddress).address
        val inboundConnection = InboundConnection(socketChannel)
        logger.info("[{}] <- [{}]", selfIp, inetAddress)
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
        while (true) {
            try {
                // ... read the socket channel.
                val lineReader = LineReader { inboundConnection.socketChannel.readSuspend(it) }
                lineReader.readLine()?.let { line ->
                    logger.info("[{}:{}]  <- [{}]", selfIp, line, inetAddress.hostAddress)
                    val event = BrokerSerializer.deserializeEventFromJson(line)
                    deliverToSubscribers(event)
                }
            } catch (ex: Exception) {
                logger.info("[{}] <-X- [{}]", selfIp, inetAddress.hostAddress)
                neighbors.updateInboundConnection(inetAddress, null)
                break
            }
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
            neighbors
                .getAll()
                .forEach { neighbor ->
                    if (!neighbor.isOutboundConnectionActive) {
                        val inetSocketAddress = InetSocketAddress(neighbor.inetAddress, COMMON_PORT)
                        try {
                            val outboundSocketChannel = AsynchronousSocketChannel.open()
                            outboundSocketChannel.connectSuspend(inetSocketAddress)
                            val updatedNeighbour = neighbor.copy(
                                outboundConnection = OutboundConnection(
                                    state = CONNECTED,
                                    inetSocketAddress = inetSocketAddress,
                                    socketChannel = outboundSocketChannel
                                )
                            )
                            neighbors.update(updatedNeighbour)
                            scope.launch {
                                writeToOutboundConnection(updatedNeighbour)
                            }

                            logger.info("[{}] -> [{}]", selfIp, neighbor.inetAddress)
                        } catch (ex: Exception) {
                            if (ex is ConnectException || ex is SocketException) {
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
     * Write to neighbour outbound connection.
     *
     * @param neighbor The neighbour of outbound connection.
     */
    private suspend fun writeToOutboundConnection(neighbor: Neighbor) {
        while (true) {
            val event = neighbor.eventQueue.dequeue(Duration.INFINITE)
            if (neighbor.isOutboundConnectionActive) {
                val outboundConnection = requireNotNull(neighbor.outboundConnection)
                val socketChannel = requireNotNull(outboundConnection.socketChannel)
                val eventJson = BrokerSerializer.serializeEventToJson(event)
                try {
                    // ... try to write the event to the socket channel.
                    socketChannel.writeSuspend(eventJson)
                    logger.info("[{}] -> '{}' -> [{}]", selfIp, eventJson, neighbor.inetAddress)
                } catch (ex: Exception) {
                    neighbors.update(
                        neighbor.copy(
                            outboundConnection = outboundConnection.copy(
                                state = DISCONNECTED,
                                socketChannel = null
                            )
                        )
                    )
                    neighbor.eventQueue.enqueue(event)
                    logger.info("[{}] -> '{}' -X-> [{}]", selfIp, eventJson, neighbor.inetAddress)
                }
            } else {
                neighbors.update(
                    neighbor.copy(
                        outboundConnection = neighbor.outboundConnection?.copy(
                            state = DISCONNECTED,
                            socketChannel = null
                        )
                    )
                )
                neighbor.eventQueue.enqueue(event)
            }
        }
    }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)

        notifyCs4kSystem("[$identifier:$selfIp] new subscriber topic '$topic' id '${subscriber.id}'")

        logger.info("[{}] new subscriber topic '{}' id '{}'", selfIp, topic, subscriber.id)

        return { unsubscribe(topic, subscriber) }
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")
        if (topic == SYSTEM_TOPIC) throw BrokerException.UnauthorizedTopicException()

        val event = Event(topic, IGNORE_EVENT_ID, message, isLastMessage)
        runBlocking {
            // Enqueue the event to be processed.
            controlQueue.enqueue(event)
        }
        notifyCs4kSystem("[$identifier:$selfIp] publish topic '$topic' event '$event'")

        logger.info("[{}] publish topic '{}' event '{}'", selfIp, topic, event)
    }

    override fun shutdown() {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")
        isShutdown = true
        scope.cancel()
        closeCoroutineDispatchers()
        brokerThread.interrupt()
        brokerThread.join()
    }

    /**
     * Unsubscribe from a topic.
     *
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic, { sub -> sub.id == subscriber.id })
        notifyCs4kSystem("[$identifier:$selfIp] unsubscribe topic '$topic' id '${subscriber.id}'")
        logger.info("[{}] unsubscribe topic '{}' id '{}'", selfIp, topic, subscriber.id)
    }

    /**
     * Notify the topic [SYSTEM_TOPIC] with the message.
     *
     * @param message The message to send.
     */
    private fun notifyCs4kSystem(message: String) {
        if (enableLogging) {
            runBlocking {
                controlQueue.enqueue(Event(SYSTEM_TOPIC, IGNORE_EVENT_ID, message, false))
            }
        }
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerIndependent::class.java)

        // Coroutine dispatcher for read operations.
        private val readCoroutineDispatcher =
            Executors.newFixedThreadPool(2).asCoroutineDispatcher()

        // Coroutine dispatcher for write operations.
        private val writeCoroutineDispatcher =
            Executors.newFixedThreadPool(2).asCoroutineDispatcher()

        //private const val COMMON_PORT = 6790
        private const val EVENTS_TO_PROCESS_CAPACITY = 5000
        private const val DEFAULT_WAIT_TIME_TO_REFRESH_NEIGHBOURS_AGAIN = 2000L
        private const val IGNORE_EVENT_ID = -1L

        /**
         * Close coroutine dispatchers in use.
         */
        private fun closeCoroutineDispatchers() {
            readCoroutineDispatcher.close()
            writeCoroutineDispatcher.close()
        }
    }
}
