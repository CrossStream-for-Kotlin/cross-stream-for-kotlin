package pt.isel.leic.cs4k.adapter

import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import pt.isel.leic.cs4k.Broker
import pt.isel.leic.cs4k.common.Event

/**
 * Extension function to subscribe to a flow of events for a given topic.
 *
 * @param topic The topic name.
 * @return A Flow of events.
 */
fun Broker.subscribeToFlow(topic: String): Flow<Event> = callbackFlow {
    val unsubscribe = this@subscribeToFlow.subscribe(topic) { event ->
        trySend(event)
        if (event.isLast) {
            close()
        }
    }

    awaitClose {
        unsubscribe()
    }
}
