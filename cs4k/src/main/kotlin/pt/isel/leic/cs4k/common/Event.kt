package pt.isel.leic.cs4k.common

/**
 * Represents an event that can be published to a topic.
 *
 * @property topic The topic of the event.
 * @property id The identifier of the event.
 * @property message The message of the event.
 * @property isLast If the event is the last one.
 */
data class Event(
    val topic: String,
    val id: Long,
    val message: String,
    val isLast: Boolean = false
) {
    /**
     * Represents the events properties.
     *
     * @property key The name of the key associated with the event property.
     */
    enum class Prop(val key: String) {
        ID("id"),
        MESSAGE("message"),
        IS_LAST("is_last");
    }
}
