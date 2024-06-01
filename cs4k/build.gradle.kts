import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.9.23"
    id("org.jlleitschuh.gradle.ktlint") version "11.6.0"

    `maven-publish` // gradle task 'publishToMavenLocal'
}

group = "pt.isel.leic.cs4k"
version = "0.0.1"

repositories {
    mavenCentral()
}

dependencies {

    // For JSON serialization and deserialization
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.1")

    // For coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")

    // For Postgresql
    implementation("org.postgresql:postgresql:42.7.0")

    // For HikariCP
    implementation("com.zaxxer:HikariCP:5.1.0")

    // For RabbitMQ
    implementation("com.rabbitmq:amqp-client:5.21.0")

    // For Redis
    implementation("redis.clients:jedis:5.1.3")
    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")

    // For logging
    implementation("ch.qos.logback:logback-classic:1.5.6")

    // For automated tests
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            groupId = group.toString()
            artifactId = "cs4k"
            version = version
        }
    }
}

tasks.named("check") {
    dependsOn("allUp")
    finalizedBy("allDown")
}

/**
 * PostgreSQl DB related tasks.
 */
task<Exec>("postgresUp") {
    commandLine("docker-compose", "up", "-d", "--build", "postgres")
}

task<Exec>("postgresWait") {
    commandLine("docker", "exec", "postgres", "/app/bin/wait-for-postgres.sh", "localhost")
    dependsOn("postgresUp")
}

task<Exec>("postgresDown") {
    commandLine("docker-compose", "-f", "docker-compose.yaml", "down")
}

/**
 * Redis related tasks.
 */
task<Exec>("redisUp") {
    commandLine("docker-compose", "up", "-d", "--build", "redis")
}

task<Exec>("redisDown") {
    commandLine("docker-compose", "down")
}

/**
 * RabbitMQ related tasks.
 */
task<Exec>("rabbitUp") {
    commandLine("docker-compose", "up", "-d", "--build", "rabbit")
}

task<Exec>("rabbitDown") {
    commandLine("docker-compose", "down")
}

/**
 * Test task, starting all containers.
 */
task<Exec>("allUp") {
    commandLine("docker-compose", "up", "-d")
}

task<Exec>("allDown") {
    commandLine("docker-compose", "down")
}
