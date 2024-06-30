package pt.isel.leic.cs4k.demo

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import pt.isel.leic.cs4k.Broker
import pt.isel.leic.cs4k.demo.Environment.CS4K_OPTION
import pt.isel.leic.cs4k.independent.BrokerIndependent
import pt.isel.leic.cs4k.independent.serviceDiscovery.config.DNSServiceDiscoveryConfig
import pt.isel.leic.cs4k.independent.serviceDiscovery.config.MulticastServiceDiscoveryConfig
import pt.isel.leic.cs4k.postgreSQL.BrokerPostgreSQL
import pt.isel.leic.cs4k.rabbitmq.BrokerRabbit
import pt.isel.leic.cs4k.rabbitmq.RabbitNode
import pt.isel.leic.cs4k.redis.BrokerRedis
import pt.isel.leic.cs4k.redis.RedisNode
import java.lang.management.ManagementFactory

@SpringBootApplication
class DemoApplication {

    private val runtimeBean = ManagementFactory.getRuntimeMXBean()
    private val pidHostInfo = runtimeBean.name.split("@")
    private val node = pidHostInfo.getOrElse(1) { "Unavailable" }

    @Bean
    fun broker(): Broker =
        when (Environment.getCS4KOption()) {
            1F -> BrokerPostgreSQL(
                Environment.getPostgreSqlDbUrl(),
                /* preventConsecutiveDuplicateEvents = true, */
                identifier = node,
                enableLogging = true
            )

            2F -> BrokerRedis(
                RedisNode(Environment.getRedisHost(), Environment.getRedisPort()),
                /* preventConsecutiveDuplicateEvents = true, */
                identifier = node,
                enableLogging = true
            )

            3F -> BrokerRabbit(
                RabbitNode(Environment.getRabbitHost(), Environment.getRabbitPort()),
                identifier = node,
                enableLogging = true
            )

            4.1F -> BrokerIndependent(
                Environment.getHostname(),
                MulticastServiceDiscoveryConfig,
                identifier = node,
                enableLogging = true
            )

            4.2F -> BrokerIndependent(
                Environment.getHostname(),
                DNSServiceDiscoveryConfig(Environment.getServiceName()),
                /* preventConsecutiveDuplicateEvents = true, */
                identifier = node,
                enableLogging = true
            )

            else -> throw Exception("Missing environment variable $CS4K_OPTION.")
        }
}

fun main(args: Array<String>) {
    runApplication<DemoApplication>(*args)
}
