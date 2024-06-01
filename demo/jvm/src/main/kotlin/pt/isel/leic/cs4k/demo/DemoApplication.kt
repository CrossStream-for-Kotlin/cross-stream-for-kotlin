package pt.isel.leic.cs4k.demo

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import pt.isel.leic.cs4k.independentBroker.BrokerIndependent
import pt.isel.leic.cs4k.independentBroker.serviceDiscovery.config.DNSServiceDiscoveryConfig
import pt.isel.leic.cs4k.independentBroker.serviceDiscovery.config.MulticastServiceDiscoveryConfig
import pt.isel.leic.cs4k.postgreSQL.BrokerPostgreSQL
import pt.isel.leic.cs4k.redis.BrokerRedis
import pt.isel.leic.cs4k.redis.RedisNode


@SpringBootApplication
class DemoApplication {

    // Option 1

    @Bean
    fun brokerPostgreSQL() = BrokerPostgreSQL(
        Environment.getPostgreSQLDbUrl()
    )

    // Option 2

    // @Bean
    fun brokerRedis() = BrokerRedis(
        RedisNode(
            Environment.getRedisHost(),
            Environment.getRedisPort()
        )
    )

    // Option 3

    // TODO(Add RabbitMQ)

    // Option 4

    // @Bean
    fun brokerIndependentMulticastServiceDiscovery() = BrokerIndependent(
        Environment.getHostname(),
        MulticastServiceDiscoveryConfig(Environment.getHostname())
    )

    // @Bean
    fun brokerIndependentDNSServiceDiscovery() = BrokerIndependent(
        Environment.getHostname(),
        DNSServiceDiscoveryConfig(Environment.getHostname(), Environment.getServiceName())
    )
}

fun main(args: Array<String>) {
    runApplication<DemoApplication>(*args)
}
