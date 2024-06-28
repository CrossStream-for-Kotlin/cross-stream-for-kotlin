package pt.isel.leic.cs4k.common

import java.util.UUID

/**
 * Represents a subscriber to a topic.
 *
 * @property id The identifier of the subscriber.
 * @property handler The handler to call when an event is published to the topic.
 * @property lastEventIdReceived The identifier of the last event the subscriber received.
 */
data class SubscriberWithEventTracking(
    override val id: UUID,
    override val handler: (event: Event) -> Unit,
    val lastEventIdReceived: Long? = null
) : BaseSubscriber
