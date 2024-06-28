package pt.isel.leic.cs4k.postgreSQL

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.postgresql.PGConnection
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.Broker
import pt.isel.leic.cs4k.Broker.Companion.SYSTEM_TOPIC
import pt.isel.leic.cs4k.Broker.Companion.UNKNOWN_IDENTIFIER
import pt.isel.leic.cs4k.common.AssociatedSubscribers
import pt.isel.leic.cs4k.common.BaseSubscriber
import pt.isel.leic.cs4k.common.BrokerException.BrokerConnectionException
import pt.isel.leic.cs4k.common.BrokerException.BrokerLostConnectionException
import pt.isel.leic.cs4k.common.BrokerException.BrokerTurnOffException
import pt.isel.leic.cs4k.common.BrokerException.UnauthorizedTopicException
import pt.isel.leic.cs4k.common.BrokerException.UnexpectedBrokerException
import pt.isel.leic.cs4k.common.BrokerSerializer
import pt.isel.leic.cs4k.common.Event
import pt.isel.leic.cs4k.common.RetryExecutor
import pt.isel.leic.cs4k.common.Subscriber
import pt.isel.leic.cs4k.common.SubscriberWithEventTracking
import pt.isel.leic.cs4k.common.Utils
import pt.isel.leic.cs4k.postgreSQL.ChannelCommandOperation.Listen
import pt.isel.leic.cs4k.postgreSQL.ChannelCommandOperation.UnListen
import java.sql.Connection
import java.sql.SQLException
import java.util.UUID
import kotlin.concurrent.thread

/**
 * Broker PostgreSQL.
 *
 * @property postgreSQLDbUrl The PostgreSQL database URL.
 * @property preventConsecutiveDuplicateEvents Prevent consecutive duplicate events.
 * @property dbConnectionPoolSize The maximum size that the JDBC connection pool is allowed to reach.
 * @property identifier Identifier of instance/node used in logs.
 * @property enableLogging Logging mode to view logs with system topic [SYSTEM_TOPIC].
 */
class BrokerPostgreSQL(
    private val postgreSQLDbUrl: String,
    private val preventConsecutiveDuplicateEvents: Boolean = false,
    private val dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE,
    private val identifier: String = UNKNOWN_IDENTIFIER,
    private val enableLogging: Boolean = false
) : Broker {

    init {
        // Check database connection pool size.
        Utils.checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Shutdown state.
    private var isShutdown = false

    // Channel to listen for notifications.
    private val channel = "cs4k_share_channel"

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Connection pool.
    private val connectionPool = retryExecutor.execute({ BrokerConnectionException() }, {
        createConnectionPool(postgreSQLDbUrl, dbConnectionPoolSize)
    })

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is SQLException && connectionPool.isClosed)
    }

    init {
        // Create the events table if it does not exist.
        createEventsTable()

        // Start a new thread to ...
        thread {
            // ... listen for notifications and ...
            listen()
            // ... wait for notifications.
            waitForNotification()
        }
    }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown) {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }

        val subscriber = createSubscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)

        getLastEvent(topic)?.let { event ->
            if (preventConsecutiveDuplicateEvents) {
                associatedSubscribers.updateLastEventIdReceived(topic, subscriber, event.id)
            }
            handler(event)
        }

        logAndNotifySystemTopic("new subscriber topic '$topic' id '${subscriber.id}'")

        return { unsubscribe(topic, subscriber) }
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown) {
            throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")
        }
        if (topic == SYSTEM_TOPIC) {
            throw UnauthorizedTopicException()
        }

        notify(topic, message, isLastMessage)
    }

    override fun shutdown() {
        if (isShutdown) {
            throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")
        }

        isShutdown = true
        unListen()
        connectionPool.close()
    }

    /**
     * Create a subscriber.
     *
     * @param id The identifier of a subscriber.
     * @param handler The handler to be called when there is a new event.
     * @return The subscriber created.
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
     */
    private fun unsubscribe(topic: String, subscriber: BaseSubscriber) {
        associatedSubscribers.removeIf(topic, { sub -> sub.id == subscriber.id })
        logAndNotifySystemTopic("unsubscribe topic '$topic' id '${subscriber.id}'")
    }

    /**
     * Wait for notifications.
     * If a new notification arrives, create an event and deliver it to subscribers.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    private fun waitForNotification() {
        try {
            retryExecutor.execute({ BrokerLostConnectionException() }, {
                connectionPool.connection.use { conn ->
                    val pgConnection = conn.unwrap(PGConnection::class.java)

                    while (!conn.isClosed) {
                        val newNotifications = pgConnection.getNotifications(BLOCK_UNTIL_NEW_NOTIFICATIONS)
                            ?: throw UnexpectedBrokerException()
                        newNotifications.forEach { notification ->
                            if (notification.name == channel) {
                                logger.info(
                                    "[{}] new event '{}' backendPid '{}'",
                                    identifier,
                                    notification.parameter,
                                    pgConnection.backendPID
                                )
                                val event = BrokerSerializer.deserializeEventFromJson(notification.parameter)
                                deliverToSubscribers(event)
                            }
                        }
                    }
                }
            }, retryCondition)
        } catch (ex: PSQLException) {
            if (!retryCondition(ex)) {
                return
            } else {
                throw ex
            }
        }
    }

    /**
     * Deliver event to subscribers, i.e., call all subscriber handlers of the event topic.
     */
    private fun deliverToSubscribers(event: Event) {
        val associatedSubscribers = if (preventConsecutiveDuplicateEvents) {
            associatedSubscribers.getAndUpdateAll(event.topic, event.id)
        } else {
            associatedSubscribers.getAll(event.topic)
        }
        associatedSubscribers.forEach { subscriber -> subscriber.handler(event) }
    }

    /**
     * Listen for notifications.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun listen() = Listen.execute()

    /**
     * UnListen for notifications.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun unListen() = UnListen.execute()

    /**
     * Execute the ChannelCommandOperation.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun ChannelCommandOperation.execute() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            connectionPool.connection.use { conn ->
                conn.createStatement().use { stm ->
                    stm.execute("$this $channel;")
                }
            }
            logger.info("[{}] $this channel '{}'", identifier, channel)
        }, retryCondition)
    }

    /**
     * Notify the topic with the message.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    private fun notify(topic: String, message: String, isLastMessage: Boolean = false) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            connectionPool.connection.use { conn ->
                try {
                    conn.autoCommit = false

                    val eventJson = BrokerSerializer.serializeEventToJson(
                        Event(
                            topic = topic,
                            id = getEventIdAndUpdateHistory(conn, topic, message, isLastMessage),
                            message = message,
                            isLast = isLastMessage
                        )
                    )

                    conn.prepareStatement("select pg_notify(?, ?)").use { stm ->
                        stm.setString(1, channel)
                        stm.setString(2, eventJson)
                        stm.execute()
                    }

                    conn.commit()
                    conn.autoCommit = true
                } catch (e: SQLException) {
                    conn.rollback()
                    throw e
                }
            }
        }, retryCondition)
        if (topic != SYSTEM_TOPIC) {
            logAndNotifySystemTopic("notify topic '$topic' event message '$message'")
        }
    }

    /**
     * Get the event id and update the history, i.e.:
     *  - If the topic does not exist, insert a new one.
     *  - If the topic exists, update the existing one.
     *
     * @param conn The connection to be used to interact with the database.
     * @param topic The topic name.
     * @param message The message.
     * @param isLast Indicates if the message is the last one.
     * @return The event id.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    private fun getEventIdAndUpdateHistory(conn: Connection, topic: String, message: String, isLast: Boolean): Long {
        conn.prepareStatement(
            """
                insert into cs4k.events (topic, message, is_last) 
                values (?, ?, ?) 
                on conflict (topic) do update 
                set id = cs4k.events.id + 1, message = excluded.message, is_last = excluded.is_last
                returning id;
            """.trimIndent()
        ).use { stm ->
            stm.setString(1, topic)
            stm.setString(2, message)
            stm.setBoolean(3, isLast)
            val rs = stm.executeQuery()
            return if (rs.next()) {
                rs.getLong("id")
            } else {
                throw UnexpectedBrokerException()
            }
        }
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
            connectionPool.connection.use { conn ->
                conn.prepareStatement("select id, message, is_last from cs4k.events where topic = ?;").use { stm ->
                    stm.setString(1, topic)
                    val rs = stm.executeQuery()
                    return@execute if (rs.next()) {
                        Event(
                            topic = topic,
                            id = rs.getLong("id"),
                            message = rs.getString("message"),
                            isLast = rs.getBoolean("is_last")
                        )
                    } else {
                        null
                    }
                }
            }
        }, retryCondition)

    /**
     * Create the events table if it does not exist.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun createEventsTable() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            connectionPool.connection.use { conn ->
                conn.createStatement().use { stm ->
                    try {
                        stm.execute(
                            """
                            create schema if not exists cs4k;
                            create table if not exists cs4k.events (
                                topic varchar(128) primary key, 
                                id integer default 0, 
                                message varchar(512), 
                                is_last boolean default false
                            );
                            """.trimIndent()
                        )
                    } catch (e: SQLException) {
                        logger.info("[{}] error with sqlstate {}", identifier, e.sqlState)
                        if (e.sqlState != UNIQUE_VIOLATION_SQLSTATE) {
                            throw e
                        } else {
                            // Here the unique violation exception is ignored.
                            // This is done because of a check-then-act done in the SQL command, but there's no
                            // harm done to the app or the database.
                            logger.info("[{}] schema and tables already created, ignoring ...", identifier)
                        }
                    }
                }
            }
        }, retryCondition)
    }

    /**
     * Log the message and notify, if logging mode is active, the topic [SYSTEM_TOPIC] with the message.
     *
     * @param message The message to send.
     */
    private fun logAndNotifySystemTopic(message: String) {
        val logMessage = "[$identifier] $message"
        logger.info(logMessage)
        if (enableLogging) {
            notify(SYSTEM_TOPIC, logMessage)
        }
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerPostgreSQL::class.java)

        // Block until new notifications arrive, using the value '0'.
        private const val BLOCK_UNTIL_NEW_NOTIFICATIONS = 0

        // SQL state of unique violation.
        private const val UNIQUE_VIOLATION_SQLSTATE = "23505"

        /**
         * Create a connection poll for database interactions.
         *
         * @param postgreSQLDbUrl The PostgreSQL database URL.
         * @param dbConnectionPoolSize The maximum size that the pool is allowed to reach.
         * @return The connection poll represented by a HikariDataSource instance.
         * @see [HikariCP](https://github.com/brettwooldridge/HikariCP)
         */
        private fun createConnectionPool(postgreSQLDbUrl: String, dbConnectionPoolSize: Int): HikariDataSource {
            val hikariConfig = HikariConfig()
            hikariConfig.jdbcUrl = postgreSQLDbUrl
            hikariConfig.maximumPoolSize = dbConnectionPoolSize
            return HikariDataSource(hikariConfig)
        }
    }
}
