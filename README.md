![cs4k_logo](cs4k_logo.png)

***Library to support the use of Server-sent events and WebSockets on multi-node backend systems***

---

## Table of Contents
* [Introduction](#introduction)
* [Artifacts](#artifacts)
  * [Gradle](#gradle)
  * [Maven](#maven)
* [Environment Variables](#environment-variables)
* [Functionalities](#functionalities)
  * [Subscribe to a topic](#subscribe-to-a-topic)
  * [Publish a message to a topic](#publish-a-message-to-a-topic)
  * [Shutdown the broker](#shutdown-the-broker)
  * [Get system topic used in logging mode to monitor library](#get-system-topic-used-in-logging-mode-to-monitor-library)
* [Configurations](#configurations)
  * [Option 1 - Using PostgreSQL](#option-1---using-postgresql)
  * [Option 2 - Using Redis](#option-2---using-redis)
    * [Redis Cluster](#redis-cluster)
    * [Redis Single Node](#redis-single-node)
  * [Option 3 - Using RabbitMQ](#option-3---using-rabbitmq)
    * [RabbitMQ Cluster](#rabbitmq-cluster)
    * [RabbitMQ Single Node](#rabbitmq-single-node)
  * [Option 4 - Without External System](#option-4---without-external-system)
* [Library guarantees](#library-guarantees)
* [Usage example](#usage-example)

---

## Introduction

In web-based architecture systems, there are front-end applications and backend systems that typically interact through 
the HTTP protocol with a request-response pattern. Frontend applications provide an interface to users, acting as the 
client. On the other hand, backend systems expose HTTP APIs, acting as the server. In this architecture, Server-sent
events and the WebSockets protocol allow servers to notify clients asynchronously. For this, long-term TCP/IP connections 
are established between clients and servers, always initiated by clients. However, these connections are not shared 
across multiple nodes in multi-node backend systems. Consequently, the use of Server-sent events and the WebSockets protocol
poses problems and challenges that hinder the development of multi-node backend systems.

**CrossStream for Kotlin** aims to reduce the complexity of developing multi-node backend systems that use Server-sent events 
or WebSockets to communicate with front-end applications. In this context, the piece of software provides four configurable 
options for communication between nodes, facilitating integration in various contexts.
In the first three options, the library uses external systems such as PostgreSQL, Redis,
and RabbitMQ for inter-node communication. In the fourth option, the library does not
depend on an external system for inter-node communication. Instead, the nodes themselves
are responsible for communication and coordination among themselves through sockets and
TCP/IP connections.

---

## Artifacts

### Gradle

```kotlin
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/CrossStream-for-Kotlin/cross-stream-for-kotlin")
        credentials {
            username = System.getenv("GITHUB_USER")
            password = System.getenv("GITHUB_TOKEN_WITH_PACKAGE_PERMISSIONS")
        }
    }
}

dependencies {
    implementation("pt.isel.leic.cs4k:cs4k:*.*.*")
}
```

[Environment Variables](#environment-variables)

---

### Maven

`pom.xml`

```xml
<project>
    <repositories>
        <repository>
            <id>github</id>
            <url>https://maven.pkg.github.com/CrossStream-for-Kotlin/cross-stream-for-kotlin</url>
        </repository>
    </repositories>
    
    <dependencies>
        <dependency>
            <groupId>pt.isel.leic.cs4k</groupId>
            <artifactId>cs4k</artifactId>
            <version>*.*.*</version>
        </dependency>
    </dependencies>
</project>
```

`settings.xml`

```xml
<settings>
    <servers>
        <server>
            <id>github</id>
            <username>${env.GITHUB_USER}</username>
            <password>${env.GITHUB_TOKEN_WITH_PACKAGE_PERMISSIONS}</password>
        </server>
    </servers>
</settings>
```

[Environment Variables](#environment-variables)

---

### Environment Variables

**Windows**

````
set GITHUB_USER=<github_username>
set GITHUB_TOKEN_WITH_PACKAGE_PERMISSIONS=<github_token_with_read:packages_permitions>
````

**Linux/Mac**

````
export GITHUB_USER=<github_username>
export GITHUB_TOKEN_WITH_PACKAGE_PERMISSIONS=<github_token_with_read:packages_permitions>
````

---

## Functionalities

All library options provide the functionalities present in the [Broker](./cs4k/src/main/kotlin/pt/isel/leic/cs4k/Broker.kt) interface/contract, although with different guarantees.
Additionally, all of these options support the use of a [FlowBrokerAdapter](./cs4k/src/main/kotlin/pt/isel/leic/cs4k/adapter/FlowBrokerAdapter.kt).

---

### Subscribe to a topic

```kotlin
interface Broker {
  fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit
}

```

- `topic` - The topic name.
- `handler` - The handler to be called when there is a new [Event](#event).

##### Event

```kotlin
data class Event(
  val topic: String,
  val id: Long,
  val message: String,
  val isLast: Boolean = false
)
```

- `topic` - The topic of the event.
- `id` - The sequential identifier of the event. The first identifier is '0' and '-1' means that the order of event identifiers should be ignored.
- `message` - The message of the event.
- `isLast` - If the event is the last one.

---

### Publish a message to a topic

```kotlin
interface Broker {
    fun publish(topic: String, message: String, isLastMessage: Boolean = false)
}
```

- `topic` - The topic name.
- `message` - The message to send.
- `isLastMessage` - Indicates if the message is the last one.

---

### Shutdown the broker

```kotlin
interface Broker {
  fun shutdown()    
}
```

---

### Get system topic used in logging mode to monitor library

```kotlin
interface Broker {
  companion object {
    const val SYSTEM_TOPIC: String 
  }
}
```

---

## Configurations

### Option 1 - Using [PostgreSQL](https://www.postgresql.org/)

```kotlin
class BrokerPostgreSQL(
    private val postgreSQLDbUrl: String,
    private val preventConsecutiveDuplicateEvents: Boolean = false,
    private val dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE,
    private val identifier: String = UNKNOWN_IDENTIFIER,
    private val enableLogging: Boolean = false,
    private val threadBuilder: Thread.Builder = Thread.ofVirtual()
) : Broker
```

- `postgreSQLDbUrl` - The PostgreSQL database URL.
- `preventConsecutiveDuplicateEvents` - Prevent consecutive duplicate events. Note that with this configuration the library's performance may be significantly affected.
- `dbConnectionPoolSize` - The maximum size that the JDBC connection pool is allowed to reach.
- `identifier` - Identifier of instance/node used in logs.
- `enableLogging` - Logging mode to view logs with system topic [SYSTEM_TOPIC](#get-system-topic-used-in-logging-mode-to-monitor-library).
- `brokerThreadType` - Thread Builder responsible for creating threads.

---

### Option 2 - Using [Redis](https://redis.io/)

#### Redis Cluster

```kotlin
class BrokerRedis(
  private val redisNodes: List<RedisNode>,
  private val preventConsecutiveDuplicateEvents: Boolean = false,
  private val dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE,
  private val identifier: String = UNKNOWN_IDENTIFIER,
  private val enableLogging: Boolean = false
) : Broker
```

- `redisNodes` - The list of [RedisNode](#redisnode).
- `preventConsecutiveDuplicateEvents` - Prevent consecutive duplicate events. Note that with this configuration the library's performance may be significantly affected.
- `dbConnectionPoolSize` - The maximum size that the connection pool is allowed to reach.
- `identifier` - Identifier of instance/node used in logs.
- `enableLogging` - Logging mode to view logs with system topic [SYSTEM_TOPIC](#get-system-topic-used-in-logging-mode-to-monitor-library).

#### Redis Single Node

```kotlin
class BrokerRedis {
  constructor(
    redisNode: RedisNode,
    preventConsecutiveDuplicateEvents: Boolean = false,
    dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE,
    identifier: String = UNKNOWN_IDENTIFIER,
    enableLogging: Boolean = false
  )
}
```

- `redisNodes` - The [RedisNode](#redisnode).
- `preventConsecutiveDuplicateEvents` - Prevent consecutive duplicate events. Note that with this configuration the library's performance may be significantly affected.
- `dbConnectionPoolSize` - The maximum size that the connection pool is allowed to reach.
- `identifier` - Identifier of instance/node used in logs.
- `enableLogging` - Logging mode to view logs with system topic [SYSTEM_TOPIC](#get-system-topic-used-in-logging-mode-to-monitor-library).

##### RedisNode

```kotlin
data class RedisNode(
  val host: String,
  val port: Int
)
```

- `host` - The host name.
- `port` - The port number.

---

### Option 3 - Using [RabbitMQ](https://www.rabbitmq.com/)

#### RabbitMQ Cluster

```kotlin
class BrokerRabbit(
  clusterNodes: List<RabbitNode>,
  username: String = DEFAULT_USERNAME,
  password: String = DEFAULT_PASSWORD,
  private val subscribeDelayInMillis: Long = DEFAULT_SUBSCRIBE_DELAY_MILLIS,
  private val identifier: String = UNKNOWN_IDENTIFIER,
  private val enableLogging: Boolean = false
) : Broker
```

- `clusterNodes` - The list of [RabbitNode](#rabbitnode).
- `username` - The username used as credentials for RabbitMQ.
- `password` - The password used as credentials for RabbitMQ.
- `subscribeDelayInMillis` - Duration of time, in milliseconds, that the broker will wait for history shared by other brokers, resulting in a delay between invocation and return.
- `identifier` - Identifier of instance/node used in logs.
- `enableLogging` - Logging mode to view logs with system topic [SYSTEM_TOPIC](#get-system-topic-used-in-logging-mode-to-monitor-library).

#### RabbitMQ Single Node

```kotlin
class BrokerRabbit {
  constructor(
    node: RabbitNode,
    username: String = DEFAULT_USERNAME,
    password: String = DEFAULT_PASSWORD,
    subscribeDelayInMillis: Long = DEFAULT_SUBSCRIBE_DELAY_MILLIS,
    identifier: String = UNKNOWN_IDENTIFIER,
    enableLogging: Boolean = false
  )
}
```

- `node` - The [RabbitNode](#rabbitnode).
- `username` - The username used as credentials for RabbitMQ.
- `password` - The password used as credentials for RabbitMQ.
- `subscribeDelayInMillis` - Duration of time, in milliseconds, that the broker will wait for history shared by other brokers, resulting in a delay between invocation and return.
- `identifier` - Identifier of instance/node used in logs.
- `enableLogging` - Logging mode to view logs with system topic [SYSTEM_TOPIC](#get-system-topic-used-in-logging-mode-to-monitor-library).

##### RabbitNode

```kotlin
data class RabbitNode(
  val host: String,
  val port: Int
)
```

- `host` - The host name.
- `port` - The port number.

---

### Option 4 - Without external system

```kotlin
class BrokerIndependent(
    private val hostname: String,
    private val serviceDiscoveryConfig: ServiceDiscoveryConfig,
    private val identifier: String = UNKNOWN_IDENTIFIER,
    private val enableLogging: Boolean = false,
    private val threadBuilder: Thread.Builder = Thread.ofVirtual()
) : Broker
```

- `hostname` - The hostname or ip address of instance/node.
- `serviceDiscoveryConfig` - [ServiceDiscoverConfig](#servicediscoverconfig).
- `identifier` - Identifier of instance/node used in logs.
- `enableLogging` - Logging mode to view logs with system topic [SYSTEM_TOPIC](#get-system-topic-used-in-logging-mode-to-monitor-library).
- `brokerThreadType` Thread Builder responsible for creating threads.

### ServiceDiscoverConfig

````kotlin
interface ServiceDiscoveryConfig {
  val periodicServiceDiscoveryUpdates: Long
}
````

- `periodicServiceDiscoveryUpdates` - Time between periodic service discovery updates.

#### Multicast Service Discovery configuration

````kotlin
data class MulticastServiceDiscoveryConfig(
  val multicastIp: String = DEFAULT_MULTICAST_IP,
  val multicastPort: Int = DEFAULT_MULTICAST_PORT,
  override val periodicServiceDiscoveryUpdates: Long = DEFAULT_SEND_DATAGRAM_PACKET_AGAIN_TIME
) : ServiceDiscoveryConfig
````

- `multicastIp` - The multicast IP.
- `multicastPort` - The multicast port.

#### DNS Service Discovery configuration

```kotlin
data class DNSServiceDiscoveryConfig(
  val serviceName: String,
  override val periodicServiceDiscoveryUpdates: Long = DEFAULT_LOOKUP_AGAIN_TIME
) : ServiceDiscoveryConfig
```
- `serviceName` - The service name.

---

## Library guarantees

| Guarantee                       | Option 1 | Option 2 | Option 3 | Option 4 |
|---------------------------------|----------|----------|----------|----------|
| Consistency                     | eventual | eventual | eventual | none     |
| Serialization                   | yes      | no       | no       | no       |
| No loss of events               | yes      | yes      | yes      | no       |
| No duplicate consecutive events | *        | *        | yes      | no       |
| Latest event of each topic      | yes      | yes      | yes      | no       |

*Depends on the configuration

---

## Usage example

In the [demo](./demo/) directory there is an example chat application in which the multi-node backend system uses 
Server-sent events to communicate with the front-end applications.