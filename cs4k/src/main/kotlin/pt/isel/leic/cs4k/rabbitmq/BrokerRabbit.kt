package pt.isel.leic.cs4k.rabbitmq

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Address
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.Broker
import pt.isel.leic.cs4k.common.AssociatedSubscribers
import pt.isel.leic.cs4k.common.BrokerException.BrokerLostConnectionException
import pt.isel.leic.cs4k.common.BrokerException.BrokerTurnOffException
import pt.isel.leic.cs4k.common.Event
import pt.isel.leic.cs4k.common.RetryExecutor
import pt.isel.leic.cs4k.common.Subscriber
import pt.isel.leic.cs4k.rabbitmq.HistoryShareMessage.HistoryShareMessageType.REQUEST
import pt.isel.leic.cs4k.rabbitmq.HistoryShareMessage.HistoryShareMessageType.RESPONSE
import java.io.IOException
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.milliseconds

class BrokerRabbit(
    clusterNodes: List<RabbitNode>,
    username: String = DEFAULT_USERNAME,
    password: String = DEFAULT_PASSWORD,
    private val subscribeDelayInMillis: Long = DEFAULT_SUBSCRIBE_DELAY_MILLIS
) : Broker {

    constructor(
        node: RabbitNode,
        username: String = DEFAULT_USERNAME,
        password: String = DEFAULT_PASSWORD,
        subscribeDelayInMillis: Long = DEFAULT_SUBSCRIBE_DELAY_MILLIS
    ) :
        this(listOf(node), username, password, subscribeDelayInMillis)

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && consumingChannelPool.isClosed && publishingChannelPool.isClosed)
    }

    // Factory where connections are created.
    private val factory = createFactory(username, password)

    // Channel pool
    private val consumingChannelPool =
        ChannelPool(factory.newConnection(clusterNodes.map { Address(it.host, it.port) }))
    private val publishingChannelPool =
        ChannelPool(factory.newConnection(clusterNodes.map { Address(it.host, it.port) }))

    // Name of stream used to publish messages to.
    private val streamName = "cs4k-notifications"

    // ID of broker used as name for the queue to receive offset requests.
    private val brokerId = "cs4k-broker:" + UUID.randomUUID().toString()

    // Exchange used to send offset requests to.
    private val historyExchange = "cs4k-history-exchange"

    // Storage for topics that are consumed, storing channel, last offset and last event.
    private val consumedTopics = ConsumedTopics()

    // Flag that indicates if broker is gracefully shutting down.
    private val isShutdown = AtomicBoolean(false)

    /**
     * Consumer used for processing messages coming from stream.
     * @param channel Channel where messages are coming from.
     */
    private inner class BrokerConsumer(channel: Channel) : DefaultConsumer(channel) {

        /**
         * Converting the received message into an event before notifying the subscribers.
         * @param message The message received from the queue.
         * @param offset The offset of the message.
         */
        private fun processMessage(message: Message, offset: Long) {
            val event = consumedTopics.createAndSetLatestEventAndOffset(
                message.topic,
                offset,
                message.message,
                message.isLast
            )
            logger.info("event received -> {}", event)
            associatedSubscribers.getAll(event.topic)
                .forEach { it.handler(event) }
        }

        override fun handleDelivery(
            consumerTag: String?,
            envelope: Envelope?,
            properties: AMQP.BasicProperties?,
            body: ByteArray?
        ) {
            requireNotNull(envelope)
            requireNotNull(properties)
            requireNotNull(body)
            val message = Message.deserialize(String(body))
            val offset = properties.headers["x-stream-offset"].toString().toLong()
            processMessage(message, offset)
            retryExecutor.execute({ BrokerLostConnectionException() }, {
                channel.basicAck(envelope.deliveryTag, false)
            }, retryCondition)
        }
    }

    /**
     * Consumer used to receive requests for offsets and events and sends them out as responses.
     * @param channel Channel where requests come from.
     */
    private inner class HistoryShareHandler(channel: Channel) : DefaultConsumer(channel) {

        // If the broker already got information from a broker through history share.
        private val gotInfoFromPeer = AtomicBoolean(false)

        /**
         * Processes the request and sends out a response including the stored offsets and events.
         * @param request The request received.
         */
        private fun handleRequest(request: HistoryShareRequest) {
            logger.info("received request from {}", request.senderQueue)
            val publishChannel = publishingChannelPool.getChannel()
            val accessInfo = consumedTopics.getAllLatestEventInfos()
            val response = HistoryShareResponse(accessInfo).toHistoryShareMessage()
            retryExecutor.execute({ BrokerLostConnectionException() }, {
                publishChannel.basicPublish(
                    "",
                    request.senderQueue,
                    null,
                    HistoryShareMessage.serialize(response).toByteArray()
                )
                publishingChannelPool.stopUsingChannel(publishChannel)
            }, retryCondition)
        }

        /**
         * Processes the response and stores all the information given.
         * @param response The response received.
         */
        private fun handleResponse(response: HistoryShareResponse) {
            if (gotInfoFromPeer.compareAndSet(false, true)) {
                logger.info("received response, storing...")
                consumedTopics.fullUpdate(response.allConsumeInfo)
                retryExecutor.execute({ BrokerLostConnectionException() }, {
                    channel.queueBind(brokerId, historyExchange, "")
                }, retryCondition)
            }
        }

        override fun handleDelivery(
            consumerTag: String?,
            envelope: Envelope?,
            properties: AMQP.BasicProperties?,
            body: ByteArray?
        ) {
            requireNotNull(envelope)
            requireNotNull(body)
            val message = HistoryShareMessage.deserialize(String(body))
            when (message.type) {
                REQUEST -> handleRequest(message.toRequest())
                RESPONSE -> handleResponse(message.toResponse())
            }
            val deliveryTag = envelope.deliveryTag
            retryExecutor.execute({ BrokerLostConnectionException() }, {
                channel.basicAck(deliveryTag, false)
            })
        }
    }

    init {
        createStream()
        createControlQueue()
        createHistoryExchange()
        fetchStoredInfoFromPeers()
        listen()
    }

    /**
     * Creating the common stream where publishes are sent.
     */
    private fun createStream() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val channel = publishingChannelPool.getChannel()
            channel.queueDeclare(
                streamName,
                true,
                false,
                false,
                mapOf(
                    "x-queue-type" to "stream",
                    "x-max-length-bytes" to DEFAULT_STREAM_SIZE
                )
            )
            publishingChannelPool.stopUsingChannel(channel)
        }, retryCondition)
    }

    /**
     * Consuming from the common stream.
     * It starts from the next message published after the consumption. However, if there were brokers consuming
     * from the stream, and they send the offset corresponding to the latest event received, it starts from there.
     */
    private fun listen() {
        val offset = consumedTopics.getMaximumOffset(subscribeDelayInMillis.milliseconds) ?: "next"
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val channel = consumingChannelPool.getChannel()
            channel.basicQos(DEFAULT_PREFETCH_VALUE)
            channel.basicConsume(
                streamName,
                false,
                mapOf(
                    "x-stream-offset" to offset
                ),
                BrokerConsumer(channel)
            )
        }, retryCondition)
    }

    /**
     * Creating the exchange used to send history requests to.
     */
    private fun createHistoryExchange() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val channel = publishingChannelPool.getChannel()
            channel.exchangeDeclare(
                historyExchange,
                "fanout"
            )
            publishingChannelPool.stopUsingChannel(channel)
        }, retryCondition)
    }

    /**
     * Creating the broker's control queue and starts consuming messages from it to start handling history requests.
     */
    private fun createControlQueue() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val channel = consumingChannelPool.getChannel()
            channel.queueDeclare(
                brokerId,
                true,
                false,
                false,
                mapOf(
                    "x-queue-type" to "quorum"
                )
            )
            channel.basicConsume(brokerId, HistoryShareHandler(channel))
        }, retryCondition)
    }

    /**
     * Requesting history from neighbouring brokers.
     */
    private fun fetchStoredInfoFromPeers() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val publishingChannel = publishingChannelPool.getChannel()
            val request = HistoryShareRequest(brokerId).toHistoryShareMessage()
            publishingChannel.basicPublish(
                historyExchange,
                "",
                null,
                HistoryShareMessage.serialize(request).toByteArray()
            )
            logger.info("started up - sending request to obtain info")
            publishingChannelPool.stopUsingChannel(publishingChannel)
        }, retryCondition)
    }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
        logger.info("new subscriber topic '{}' id '{}'", topic, subscriber.id)

        getLastEvent(topic)?.let { event -> handler(event) }

        return { unsubscribe(topic, subscriber) }
    }

    /**
     * Obtain the last event of the topic stored within the broker.
     * @param topic The topic of the desired event.
     */
    private fun getLastEvent(topic: String) = consumedTopics.getLatestEvent(topic)

    /**
     * Canceling a previously-made subscription.
     * @param topic The topic previously subscribed to.
     * @param subscriber The subscriber wanting ot cancel the subscription.
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic, { it.id.toString() == subscriber.id.toString() })
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        val body = Message.serialize(Message(topic, message, isLastMessage)).toByteArray()
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val channel = publishingChannelPool.getChannel()
            channel.basicPublish("", streamName, null, body)
            publishingChannelPool.stopUsingChannel(channel)
        }, retryCondition)
    }

    override fun shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            consumingChannelPool.close()
            publishingChannelPool.close()
            consumedTopics.removeAll()
        } else {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbit::class.java)

        // Default value for QoS or prefetch for un-acked messages, required for stream consumption.
        private const val DEFAULT_PREFETCH_VALUE = 100

        // Default value for subscription delay, used as a timeout waiting for receiving history from another
        // broker.
        private const val DEFAULT_SUBSCRIBE_DELAY_MILLIS = 250L

        // Default value for the common stream size, in bytes.
        private const val DEFAULT_STREAM_SIZE = 8_000_000

        // Default credentials to access RabbitMQ.
        private const val DEFAULT_USERNAME = "guest"
        private const val DEFAULT_PASSWORD = "guest"

        /**
         * Creates the creator of connections for accessing RabbitMQ broker.
         */
        private fun createFactory(username: String, password: String): ConnectionFactory {
            val factory = ConnectionFactory()
            factory.username = username
            factory.password = password
            return factory
        }
    }
}
