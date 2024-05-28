package pt.isel.leic.cs4k.common

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Responsible for managing the association between topics and [BaseSubscriber]s.
 * It is thread-safe.
 */
class AssociatedSubscribers {

    // Map that associates topics with lists of subscribers.
    private val map = HashMap<String, List<BaseSubscriber>>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Get all subscribers associated with a topic.
     *
     * @param topic The topic to get the subscribers from.
     * @return The list of subscribers associated with the topic.
     */
    fun getAll(topic: String) = lock.withLock {
        map[topic] ?: emptyList()
    }

    /**
     * Check if there are no subscribers for a given topic.
     *
     * @param topic The topic to check.
     * @return True if there are no subscribers for a given topic.
     */
    fun noSubscribers(topic: String) = lock.withLock {
        map[topic].isNullOrEmpty()
    }

    /**
     * Add a subscriber to a topic.
     *
     * @param topic The topic to add the subscriber to.
     * @param subscriber The subscriber to add.
     * @param onTopicAdd Method to be executed only if the subscriber subscribes to a new topic.
     */
    fun addToKey(topic: String, subscriber: BaseSubscriber, onTopicAdd: (() -> Unit)? = null) {
        var newTopic = false
        lock.withLock {
            map.compute(topic) { _, subscribers ->
                if (subscribers == null) {
                    newTopic = true
                    listOf(subscriber)
                } else {
                    subscribers + subscriber
                }
            }
        }
        if (onTopicAdd != null && newTopic) onTopicAdd()
    }

    /**
     * Remove a subscriber from a topic.
     *
     * @param topic The topic to remove the subscriber from.
     * @param predicate A predicate to determine which subscriber to remove.
     * @param onTopicRemove Method to be executed only if there are no more subscribers to the topic.
     */
    fun removeIf(topic: String, predicate: (BaseSubscriber) -> Boolean, onTopicRemove: (() -> Unit)? = null) {
        var topicGone = false
        lock.withLock {
            map.computeIfPresent(topic) { _, subscribers ->
                val subscriberToRemove = subscribers.find(predicate) ?: return@computeIfPresent subscribers
                (subscribers - subscriberToRemove).ifEmpty {
                    topicGone = true
                    null
                }
            }
        }
        if (onTopicRemove != null && topicGone) onTopicRemove()
    }
}
