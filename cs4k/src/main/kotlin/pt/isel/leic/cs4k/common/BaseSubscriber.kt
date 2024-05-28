package pt.isel.leic.cs4k.common

import java.util.UUID

/**
 * Public contract for the base representation of a subscriber.
 *
 * @property id The identifier of the subscriber.
 * @property handler The handler to call when an event is published to the topic.
 */
interface BaseSubscriber {
    val id: UUID
    val handler: (event: Event) -> Unit
}
