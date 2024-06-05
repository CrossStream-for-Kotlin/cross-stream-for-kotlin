package pt.isel.leic.cs4k.rabbitmq

import org.junit.jupiter.api.assertDoesNotThrow
import pt.isel.leic.cs4k.common.BrokerException
import pt.isel.leic.cs4k.utils.Environment
import kotlin.test.Test
import kotlin.test.assertFailsWith

class BrokerRabbitTests {

    @Test
    fun `can create a BrokerRabbit with a node`() {
        assertDoesNotThrow {
            BrokerRabbit(
                RabbitNode(Environment.getRabbitHost(), Environment.getRabbitPort())
            ).shutdown()
        }
    }

    @Test
    fun `cannot create a BrokerRabbit with an empty cluster list`() {
        assertFailsWith<BrokerException.NodeListIsEmptyException> {
            BrokerRabbit(
                emptyList()
            )
        }
    }
}
