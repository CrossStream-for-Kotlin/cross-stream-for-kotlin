package pt.isel.leic.cs4k.adapter

import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import org.slf4j.LoggerFactory
import pt.isel.leic.cs4k.Broker
import pt.isel.leic.cs4k.common.Event

class FlowBrokerAdapter(private val broker: Broker) {

    /* fun subscribeToFlow(topic: String): Flow<Event> {
         val channel = Channel<Event>(Channel.UNLIMITED)
         val unsubscribe = broker.subscribe(topic) { event ->
             runBlocking {
                 logger.info("Sending event to channel")
                 channel.send(event)
             }
         }

         return channel.consumeAsFlow()
             .onCompletion {
                 unsubscribe()
                 channel.close()
             }
     }*/

    fun subscribeToFlow(topic: String): Flow<Event> = callbackFlow {
        val unsubscribe = broker.subscribe(topic) { event ->
            logger.info("Sending event to channel")
            this@callbackFlow.trySend(event)
        }

        awaitClose {
            unsubscribe()
        }
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(FlowBrokerAdapter::class.java)
    }
}
