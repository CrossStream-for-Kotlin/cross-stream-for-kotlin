package pt.isel.leic.cs4k.demo

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import pt.isel.leic.cs4k.postgreSQL.BrokerPostgreSQL
import pt.isel.leic.cs4k.redis.BrokerRedis
import pt.isel.leic.cs4k.redis.RedisNode

@SpringBootApplication
class DemoApplication {

    @Bean
    fun brokerPostgreSQL() = BrokerPostgreSQL(
        Environment.getPostgreSQLDbUrl()
    )

    // @Bean
    fun brokerRedis() = BrokerRedis(
        RedisNode(
            Environment.getRedisHost(),
            Environment.getRedisPort()
        )
    )
}

fun main(args: Array<String>) {
    runApplication<DemoApplication>(*args)
}
