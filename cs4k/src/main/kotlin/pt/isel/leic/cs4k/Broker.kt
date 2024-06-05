package pt.isel.leic.cs4k

import pt.isel.leic.cs4k.common.BrokerException.BrokerLostConnectionException
import pt.isel.leic.cs4k.common.BrokerException.BrokerTurnOffException
import pt.isel.leic.cs4k.common.BrokerException.UnauthorizedTopicException
import pt.isel.leic.cs4k.common.BrokerException.UnexpectedBrokerException
import pt.isel.leic.cs4k.common.Event

/**
 * Public contract of Broker.
 */
interface Broker {

    /**
     * Subscribe to a topic.
     *
     * @param topic The topic name.
     * @param handler The handler to be called when there is a new event.
     * @return The method to be called when unsubscribing.
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the external system.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit

    /**
     * Publish a message to a topic.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the external system.
     * @throws UnexpectedBrokerException If something unexpected happens.
     * @throws UnauthorizedTopicException If the subscribed topic is [SYSTEM_TOPIC].
     *
     */
    fun publish(topic: String, message: String, isLastMessage: Boolean = false)

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the external system.
     */
    fun shutdown()

    companion object {
        // System topic for logging.
        const val SYSTEM_TOPIC = "cs4k_system"
    }
}
