package pt.isel.leic.cs4k.redis

import io.lettuce.core.AbstractRedisClient
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisException
import io.lettuce.core.RedisURI
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulConnection
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.pubsub.RedisPubSubAdapter
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.Broker
import pt.isel.leic.cs4k.Broker.Companion.SYSTEM_TOPIC
import pt.isel.leic.cs4k.Broker.Companion.UNKNOWN_IDENTIFIER
import pt.isel.leic.cs4k.common.AssociatedSubscribers
import pt.isel.leic.cs4k.common.BaseSubscriber
import pt.isel.leic.cs4k.common.BrokerException.BrokerConnectionException
import pt.isel.leic.cs4k.common.BrokerException.BrokerLostConnectionException
import pt.isel.leic.cs4k.common.BrokerException.BrokerTurnOffException
import pt.isel.leic.cs4k.common.BrokerException.NodeListIsEmptyException
import pt.isel.leic.cs4k.common.BrokerException.UnauthorizedTopicException
import pt.isel.leic.cs4k.common.BrokerException.UnexpectedBrokerException
import pt.isel.leic.cs4k.common.BrokerSerializer
import pt.isel.leic.cs4k.common.Event
import pt.isel.leic.cs4k.common.RetryExecutor
import pt.isel.leic.cs4k.common.Subscriber
import pt.isel.leic.cs4k.common.SubscriberWithEventTracking
import pt.isel.leic.cs4k.common.Utils
import java.util.UUID

/**
 * Broker Redis - Cluster.
 *
 * @property redisNodes The list of Redis nodes.
 * @property preventConsecutiveDuplicateEvents Prevent consecutive duplicate events.
 * @property dbConnectionPoolSize The maximum size that the connection pool is allowed to reach.
 * @property identifier Identifier of instance/node used in logs.
 * @property enableLogging Logging mode to view logs with system topic [SYSTEM_TOPIC].
 */
class BrokerRedis(
    private val redisNodes: List<RedisNode>,
    private val preventConsecutiveDuplicateEvents: Boolean = false,
    private val dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE,
    private val identifier: String = UNKNOWN_IDENTIFIER,
    private val enableLogging: Boolean = false
) : Broker {

    /**
     * Broker Redis - Single Node.
     *
     * @property redisNode The Redis node.
     * @property preventConsecutiveDuplicateEvents Prevent consecutive duplicate events.
     * @property dbConnectionPoolSize The maximum size that the connection pool is allowed to reach.
     * @property identifier Identifier of instance/node used in logs.
     * @property enableLogging Logging mode to view logs with system topic [SYSTEM_TOPIC].
     */
    constructor(
        redisNode: RedisNode,
        preventConsecutiveDuplicateEvents: Boolean = false,
        dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE,
        identifier: String = UNKNOWN_IDENTIFIER,
        enableLogging: Boolean = false
    ) :
        this(listOf(redisNode), preventConsecutiveDuplicateEvents, dbConnectionPoolSize, identifier, enableLogging)

    init {
        // Check database connection pool size.
        Utils.checkDbConnectionPoolSize(dbConnectionPoolSize)

        // Check node list.
        if (redisNodes.isEmpty()) {
            throw NodeListIsEmptyException()
        }
    }

    // Check if it is cluster.
    private val isCluster: Boolean
        get() = redisNodes.size > 1

    // Shutdown state.
    private var isShutdown = false

    // Prefix for all keys and channels to mitigate possible conflicts with other contexts.
    private val prefix = "cs4k_"

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Redis client.
    private val redisClient = retryExecutor.execute({ BrokerConnectionException() }, {
        if (isCluster) {
            createRedisClusterClient(redisNodes)
        } else {
            createRedisClient(redisNodes.first())
        }
    })

    // Connection to asynchronous subscribe and unsubscribe.
    private val subConnection = retryExecutor.execute({ BrokerConnectionException() }, {
        createConnectPubSub(redisClient)
    })

    // Connection to asynchronous publish.
    private val pubConnection = retryExecutor.execute({ BrokerConnectionException() }, {
        createConnectPubSub(redisClient)
    })

    // Connection pool.
    private val connectionPool = retryExecutor.execute({ BrokerConnectionException() }, {
        createConnectionPool(redisClient, dbConnectionPoolSize)
    })

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is RedisException && (!subConnection.isOpen || !pubConnection.isOpen || connectionPool.isClosed))
    }

    private val singletonRedisPubSubAdapter = object : RedisPubSubAdapter<String, String>() {

        override fun message(channel: String?, message: String?) {
            if (channel == null || message == null) {
                throw UnexpectedBrokerException()
            }
            logger.info("[{}] new event '{}' channel '{}'", identifier, message, channel)

            processMessage(channel.substringAfter(prefix), message)
        }
    }

    init {
        // Add a listener.
        subConnection.addListener(singletonRedisPubSubAdapter)
    }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown) {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }

        val subscriber = createSubscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber) { subscribeTopic(topic) }

        getLastEvent(topic)?.let { event ->
            if (preventConsecutiveDuplicateEvents) {
                associatedSubscribers.updateLastEventIdReceived(topic, subscriber.id, event.id)
            }
            handler(event)
        }

        logAndPublishMessageToSystemTopic("new subscriber topic '$topic' id '${subscriber.id}'")

        return { unsubscribe(topic, subscriber) }
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown) {
            throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")
        }
        if (topic == SYSTEM_TOPIC) {
            throw UnauthorizedTopicException()
        }

        publishMessage(topic, message, isLastMessage)
    }

    override fun shutdown() {
        if (isShutdown) {
            throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")
        }

        isShutdown = true
        subConnection.removeListener(singletonRedisPubSubAdapter)
        subConnection.close()
        pubConnection.close()
        connectionPool.close()
        redisClient.shutdown()
    }

    /**
     * Process the message received from Redis.
     *
     * @param topic The topic.
     * @param message The message.
     */
    private fun processMessage(topic: String, message: String) {
        if (preventConsecutiveDuplicateEvents) {
            val event = BrokerSerializer.deserializeEventFromJson(message)
            val associatedSubscribers = associatedSubscribers.getAndUpdateAll(topic, event.id)
            if (associatedSubscribers.isNotEmpty()) {
                deliverToSubscribers(associatedSubscribers, event)
            }
        } else {
            val associatedSubscribers = associatedSubscribers.getAll(topic)
            if (associatedSubscribers.isNotEmpty()) {
                deliverToSubscribers(associatedSubscribers, BrokerSerializer.deserializeEventFromJson(message))
            }
        }
    }

    /**
     * Deliver event to subscribers, i.e., call all subscriber handlers of the event topic.
     *
     * @param subscribers The list of subscribers to call the handler.
     * @param event The event.
     */
    private fun deliverToSubscribers(subscribers: List<BaseSubscriber>, event: Event) {
        subscribers.forEach { subscriber -> subscriber.handler(event) }
    }

    /**
     * Create a subscriber.
     *
     * @param id The identifier of a subscriber.
     * @param handler The handler to be called when there is a new event.
     * @return The created subscriber created.
     */
    private fun createSubscriber(id: UUID, handler: (event: Event) -> Unit) =
        if (preventConsecutiveDuplicateEvents) {
            SubscriberWithEventTracking(id, handler)
        } else {
            Subscriber(id, handler)
        }

    /**
     * Unsubscribe from a topic.
     *
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun unsubscribe(topic: String, subscriber: BaseSubscriber) {
        associatedSubscribers.removeIf(
            topic = topic,
            predicate = { sub -> sub.id == subscriber.id },
            onTopicRemove = { unsubscribeTopic(topic) }
        )
        logAndPublishMessageToSystemTopic("unsubscribe topic '$topic' id '${subscriber.id}'")
    }

    /**
     * Subscribe topic.
     *
     * @param topic The topic name.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun subscribeTopic(topic: String) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            logger.info("[{}] subscribe new topic '{}'", identifier, prefix + topic)
            subConnection.async().subscribe(prefix + topic)
        }, retryCondition)
    }

    /**
     * UnSubscribe topic.
     *
     * @param topic The topic name.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun unsubscribeTopic(topic: String) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            logger.info("[{}] unsubscribe gone topic '{}'", identifier, prefix + topic)
            subConnection.async().unsubscribe(prefix + topic)
        }, retryCondition)
    }

    /**
     * Publish a message to a topic.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     * @throws UnexpectedBrokerException If an invalid state exists.
     */
    private fun publishMessage(topic: String, message: String, isLastMessage: Boolean = false) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val eventJson = BrokerSerializer.serializeEventToJson(
                Event(
                    topic = topic,
                    id = getEventIdAndUpdateHistory(topic, message, isLastMessage),
                    message = message,
                    isLast = isLastMessage
                )
            )
            pubConnection.async().publish(prefix + topic, eventJson)
        }, retryCondition)
        if (topic != SYSTEM_TOPIC) {
            logAndPublishMessageToSystemTopic("publish topic '$topic' event message '$message'")
        }
    }

    /**
     * Get the event id and update the history, i.e.:
     *  - If the topic does not exist, insert a new one.
     *  - If the topic exists, update the existing one.
     *
     * @param topic The topic name.
     * @param message The message.
     * @param isLast Indicates if the message is the last one.
     * @return The event id.
     * @throws UnexpectedBrokerException If an invalid state exists.
     */
    private fun getEventIdAndUpdateHistory(topic: String, message: String, isLast: Boolean): Long =
        connectionPool.borrowObject().use { conn ->
            getSyncCommands(conn).eval(
                GET_EVENT_ID_AND_UPDATE_HISTORY_SCRIPT,
                ScriptOutputType.INTEGER,
                arrayOf(prefix + topic),
                Event.Prop.ID.key,
                Event.Prop.MESSAGE.key,
                message,
                Event.Prop.IS_LAST.key,
                isLast.toString()
            )
        }

    /**
     * Get the last event from the topic.
     *
     * @param topic The topic name.
     * @return The last event of the topic, or null if the topic does not exist yet.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     * @throws UnexpectedBrokerException If an invalid state exists.
     */
    private fun getLastEvent(topic: String): Event? =
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val map = connectionPool.borrowObject().use { conn ->
                getSyncCommands(conn).hgetall(prefix + topic)
            }
            val id = map[Event.Prop.ID.key]?.toLong()
            val message = map[Event.Prop.MESSAGE.key]
            val isLast = map[Event.Prop.IS_LAST.key]?.toBoolean()
            return@execute if (id != null && message != null && isLast != null) {
                Event(topic, id, message, isLast)
            } else {
                null
            }
        }, retryCondition)

    /**
     * Get the commands for synchronous API.
     *
     * @param conn The connection to use.
     * @return The commands for synchronous API.
     * @throws UnexpectedBrokerException If an invalid state exists.
     */
    private fun getSyncCommands(conn: StatefulConnection<String, String>) =
        when (conn) {
            is StatefulRedisConnection -> conn.sync()
            is StatefulRedisClusterConnection -> conn.sync()
            else -> throw UnexpectedBrokerException(UNSUPPORTED_STATE_DEFAULT_MESSAGE)
        }

    /**
     * Log the message and publish, if logging mode is active, the topic [SYSTEM_TOPIC] with the message.
     *
     * @param message The message to send.
     */
    private fun logAndPublishMessageToSystemTopic(message: String) {
        val logMessage = "[$identifier] $message"
        logger.info(logMessage)
        if (enableLogging) {
            publishMessage(SYSTEM_TOPIC, logMessage)
        }
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRedis::class.java)

        // Script to atomically update history and get the identifier for the event.
        private val GET_EVENT_ID_AND_UPDATE_HISTORY_SCRIPT = """
            redis.call('hsetnx', KEYS[1], ARGV[1], '-1')
            local id = redis.call('hincrby', KEYS[1], ARGV[1], 1)
            redis.call('hmset', KEYS[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])
            return id
        """.trimIndent()

        // Unsupported state default message.
        private const val UNSUPPORTED_STATE_DEFAULT_MESSAGE = "Unsupported state."

        /**
         * Create a redis client for database interactions.
         *
         * @param redisNode The Redis node.
         * @return The redis client instance.
         */
        private fun createRedisClient(redisNode: RedisNode) =
            RedisClient.create(RedisURI.create(redisNode.host, redisNode.port))

        /**
         * Create a redis cluster client for database interactions.
         *
         * @param redisNodes The list of Redis nodes.
         * @return The redis cluster client instance.
         */
        private fun createRedisClusterClient(redisNodes: List<RedisNode>) =
            RedisClusterClient.create(
                redisNodes.map { redisNode ->
                    RedisURI.Builder.redis(redisNode.host, redisNode.port).build()
                }
            )

        /**
         * Create a connection poll for database interactions.
         *
         * @param dbConnectionPoolSize The maximum size that the pool is allowed to reach.
         * @param redisClient The redis client instance.
         * @return The connection poll represented by a GenericObjectPool instance.
         * @throws UnexpectedBrokerException If an invalid state exists.
         */
        private fun createConnectionPool(redisClient: AbstractRedisClient, dbConnectionPoolSize: Int) =
            when (redisClient) {
                is RedisClient -> createSingleNodeConnectionPool(dbConnectionPoolSize, redisClient)
                is RedisClusterClient -> createClusterConnectionPool(dbConnectionPoolSize, redisClient)
                else -> throw UnexpectedBrokerException(UNSUPPORTED_STATE_DEFAULT_MESSAGE)
            }

        /**
         * Create a single node connection poll for database interactions.
         *
         * @param dbConnectionPoolSize The maximum size that the pool is allowed to reach.
         * @param client The redis client instance.
         * @return The connection poll represented by a GenericObjectPool instance.
         */
        private fun createSingleNodeConnectionPool(
            dbConnectionPoolSize: Int,
            client: RedisClient
        ): GenericObjectPool<StatefulRedisConnection<String, String>> {
            val pool = GenericObjectPoolConfig<StatefulRedisConnection<String, String>>()
            pool.maxTotal = dbConnectionPoolSize
            return ConnectionPoolSupport.createGenericObjectPool({ client.connect() }, pool)
        }

        /**
         * Create a cluster connection poll for database interactions.
         *
         * @param dbConnectionPoolSize The maximum size that the pool is allowed to reach.
         * @param clusterClient The redis cluster client instance.
         * @return The cluster connection poll represented by a GenericObjectPool instance.
         */
        private fun createClusterConnectionPool(
            dbConnectionPoolSize: Int,
            clusterClient: RedisClusterClient
        ): GenericObjectPool<StatefulRedisClusterConnection<String, String>> {
            val pool = GenericObjectPoolConfig<StatefulRedisClusterConnection<String, String>>()
            pool.maxTotal = dbConnectionPoolSize
            return ConnectionPoolSupport.createGenericObjectPool({ clusterClient.connect() }, pool)
        }

        /**
         * Create a connection for publish-subscribe operations.
         *
         * @param redisClient The redis client instance.
         * @return The connectPubSub represented by a StatefulRedisPubSubConnection instance.
         * @throws UnexpectedBrokerException If an invalid state exists.
         */
        private fun createConnectPubSub(redisClient: AbstractRedisClient) =
            when (redisClient) {
                is RedisClient -> redisClient.connectPubSub()
                is RedisClusterClient -> redisClient.connectPubSub()
                else -> throw UnexpectedBrokerException(UNSUPPORTED_STATE_DEFAULT_MESSAGE)
            }
    }
}
