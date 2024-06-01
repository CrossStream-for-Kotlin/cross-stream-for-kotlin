package pt.isel.leic.cs4k.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisException
import io.lettuce.core.RedisURI
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands
import io.lettuce.core.cluster.api.sync.RedisClusterCommands
import io.lettuce.core.pubsub.RedisPubSubAdapter
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.Broker
import pt.isel.leic.cs4k.common.AssociatedSubscribers
import pt.isel.leic.cs4k.common.BrokerException.BrokerConnectionException
import pt.isel.leic.cs4k.common.BrokerException.BrokerLostConnectionException
import pt.isel.leic.cs4k.common.BrokerException.BrokerTurnOffException
import pt.isel.leic.cs4k.common.BrokerException.UnexpectedBrokerException
import pt.isel.leic.cs4k.common.BrokerSerializer
import pt.isel.leic.cs4k.common.Event
import pt.isel.leic.cs4k.common.RetryExecutor
import pt.isel.leic.cs4k.common.Subscriber
import pt.isel.leic.cs4k.common.Utils
import java.util.*

/**
 * Broker Redis - Cluster.
 *
 * @property redisNodes The list of Redis nodes.
 * @property dbConnectionPoolSize The maximum size that the connection pool is allowed to reach.
 */
class BrokerRedis(
    private val redisNodes: List<RedisNode>,
    private val dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE
) : Broker {

    /**
     * Broker Redis - Single Node.
     *
     * @property redisNode The Redis node.
     * @property dbConnectionPoolSize The maximum size that the connection pool is allowed to reach.
     */
    constructor(
        redisNode: RedisNode,
        dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE
    ) :
            this(listOf(redisNode), dbConnectionPoolSize)

    init {
        // Check database connection pool size.
        Utils.checkDbConnectionPoolSize(dbConnectionPoolSize)
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

    /*
    // Redis client.
    private val redisClient = retryExecutor.execute({ BrokerConnectionException() }, {
        if (isCluster) {
            createRedisClusterClient(redisNodes)
        } else {
            createRedisClient(redisNodes.first())
        }
    })

     */

    private val redisClient = retryExecutor.execute({ BrokerConnectionException() }, {
        if (!isCluster) createRedisClient(redisNodes.first()) else null
    })

    private val redisClusterClient = retryExecutor.execute({ BrokerConnectionException() }, {
        if (isCluster) createRedisClusterClient(redisNodes) else null
    })

    // Connection to asynchronous subscribe, unsubscribe and publish.
    private val pubSubConnection = retryExecutor.execute({ BrokerConnectionException() }, {
        if (!isCluster) redisClient?.connectPubSub() else null
    })

    private val pubSubClusterConnection = retryExecutor.execute({ BrokerConnectionException() }, {
        if (isCluster) redisClusterClient?.connectPubSub() else null
    })

    // Connection pool.
    private val connectionPool: GenericObjectPool<StatefulRedisConnection<String, String>>? =
        retryExecutor.execute({ BrokerConnectionException() }, {
            if (!isCluster) redisClient?.let { createConnectionPool(dbConnectionPoolSize, it) } else null
        })

    private val clusterConnectionPool: GenericObjectPool<StatefulRedisClusterConnection<String, String>>? =
        retryExecutor.execute({ BrokerConnectionException() }, {
            if (!isCluster) redisClusterClient?.let { createClusterConnectionPool(dbConnectionPoolSize, it) } else null
        })

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is RedisException &&
                ((pubSubConnection != null && connectionPool != null &&
                        (!pubSubConnection.isOpen || connectionPool.isClosed) ||
                        (pubSubClusterConnection != null && clusterConnectionPool != null &&
                                (!pubSubClusterConnection.isOpen || clusterConnectionPool.isClosed)))
                        ))
    }

    private val singletonRedisPubSubAdapter = object : RedisPubSubAdapter<String, String>() {

        override fun message(channel: String?, message: String?) {
            if (channel == null || message == null) throw UnexpectedBrokerException()
            logger.info("new event '{}' channel '{}'", message, channel)

            val subscribers = associatedSubscribers.getAll(channel.substringAfter(prefix))
            if (subscribers.isNotEmpty()) {
                val event = BrokerSerializer.deserializeEventFromJson(message)
                subscribers.forEach { subscriber -> subscriber.handler(event) }
            }
        }
    }

    init {
        // Add a listener.
        if (isCluster)
            pubSubClusterConnection?.addListener(singletonRedisPubSubAdapter)
        else
            pubSubConnection?.addListener(singletonRedisPubSubAdapter)
    }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber) {
            subscribeTopic(topic)
        }
        logger.info("new subscriber topic '{}' id '{}'", topic, subscriber.id)

        getLastEvent(topic)?.let { event -> handler(event) }

        return { unsubscribe(topic, subscriber) }
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")

        publishMessage(topic, message, isLastMessage)
    }

    override fun shutdown() {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")
        isShutdown = true
        if (isCluster) {
            pubSubClusterConnection?.removeListener(singletonRedisPubSubAdapter)
            pubSubClusterConnection?.close()
            clusterConnectionPool?.close()
            redisClusterClient?.shutdown()
        } else {
            pubSubConnection?.removeListener(singletonRedisPubSubAdapter)
            pubSubConnection?.close()
            connectionPool?.close()
            redisClient?.shutdown()
        }

    }

    /**
     * Unsubscribe from a topic.
     *
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(
            topic = topic,
            predicate = { sub -> sub.id == subscriber.id },
            onTopicRemove = { unsubscribeTopic(topic) }
        )
        logger.info("unsubscribe topic '{}' id '{}'", topic, subscriber.id)
    }

    /**
     * Subscribe topic.
     *
     * @param topic The topic name.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun subscribeTopic(topic: String) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            logger.info("subscribe new topic '{}'", prefix + topic)
            if (isCluster)
                pubSubClusterConnection?.async()?.subscribe(prefix + topic)
            else
                pubSubConnection?.async()?.subscribe(prefix + topic)
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
            logger.info("unsubscribe gone topic '{}'", prefix + topic)
            if (isCluster)
                pubSubClusterConnection?.async()?.unsubscribe(prefix + topic)
            else
                pubSubConnection?.async()?.unsubscribe(prefix + topic)
        }, retryCondition)
    }

    /**
     * Publish a message to a topic.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun publishMessage(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val eventJson = BrokerSerializer.serializeEventToJson(
                Event(
                    topic = topic,
                    id = getEventIdAndUpdateHistory(topic, message, isLastMessage),
                    message = message,
                    isLast = isLastMessage
                )
            )
            if (isCluster)
                pubSubClusterConnection?.async()?.publish(prefix + topic, eventJson)
            else {
                pubSubConnection?.async()?.publish(prefix + topic, eventJson)
            }
            logger.info("publish topic '{}' event '{}'", topic, eventJson)
        }, retryCondition)
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
     */
    private fun getEventIdAndUpdateHistory(topic: String, message: String, isLast: Boolean): Long {
        val id = if (isCluster) {
            clusterConnectionPool?.let {
                it.borrowObject().use { conn ->
                    conn.sync().eval<Long>(
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
            }
        } else {
            connectionPool?.let {
                it.borrowObject().use { conn ->
                    val syncCommands = conn.sync()
                    syncCommands.eval<Long>(
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
            }
        }
        return id!!
    }


    /**
     * Get the last event from the topic.
     *
     * @param topic The topic name.
     * @return The last event of the topic, or null if the topic does not exist yet.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun getLastEvent(topic: String): Event? =
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            if (isCluster) {
                clusterConnectionPool?.let {
                    val map = it.borrowObject().use { conn ->
                        conn.sync().hgetall(prefix + topic)
                    }
                    val id = map[Event.Prop.ID.key]?.toLong()
                    val message = map[Event.Prop.MESSAGE.key]
                    val isLast = map[Event.Prop.IS_LAST.key]?.toBoolean()
                    return@execute if (id != null && message != null && isLast != null) {
                        Event(topic, id, message, isLast)
                    } else {
                        null
                    }
                }
            } else {
                connectionPool?.let {
                    val map = it.borrowObject().use { conn ->
                        conn.sync().hgetall(prefix + topic)
                    }
                    val id = map[Event.Prop.ID.key]?.toLong()
                    val message = map[Event.Prop.MESSAGE.key]
                    val isLast = map[Event.Prop.IS_LAST.key]?.toBoolean()
                    return@execute if (id != null && message != null && isLast != null) {
                        Event(topic, id, message, isLast)
                    } else {
                        null
                    }
                }
            }
        }, retryCondition)

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
         * @param client The redis client instance.
         * @return The connection poll represented by a GenericObjectPool instance.
         */
        private fun createConnectionPool(
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
    }
}
