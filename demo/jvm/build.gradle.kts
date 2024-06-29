plugins {
    kotlin("jvm") version "1.9.23"
    kotlin("plugin.spring") version "1.8.22"

    id("org.springframework.boot") version "3.1.4"
    id("io.spring.dependency-management") version "1.1.3"
    id("org.jlleitschuh.gradle.ktlint") version "11.6.0"
}

group = "pt.isel.leic.cs4k.demo"
version = "0.0.1"

repositories {
    mavenLocal()
    mavenCentral()

    // For CS4K Library.
    maven {
        url = uri("https://maven.pkg.github.com/CrossStream-for-Kotlin/cross-stream-for-kotlin")
        credentials {
            username = System.getenv("GITHUB_USER")
            password = System.getenv("GITHUB_TOKEN_WITH_PACKAGE_PERMISSIONS")
        }
    }
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.apache.tomcat.embed:tomcat-embed-core:10.1.17")
    implementation("org.apache.tomcat.embed:tomcat-embed-websocket:10.1.17")
    implementation("org.apache.tomcat.embed:tomcat-embed-el:10.1.17")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")

    testImplementation(kotlin("test"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webflux")

    // For CS4K Library.
    implementation("pt.isel.leic.cs4k:cs4k:0.1.7")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(21)
}

/**
 * Demo related tasks
 */
task<Copy>("extractUberJar") {
    dependsOn("assemble")
    // opens the JAR containing everything...
    from(zipTree("$buildDir/libs/jvm-$version.jar"))
    // ... into the 'build/dependency' folder
    into("build/dependency")
}

// Option 1 - PostgreSQL

task<Exec>("demoOption1PostgresComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "docker-compose-demo-option1-postgres.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=3"
    )
    dependsOn("extractUberJar")
}

task<Exec>("demoOption1PostgresComposeDown") {
    commandLine("docker-compose", "-f", "docker-compose-demo-option1-postgres.yaml", "down")
}

// Option 2 - Redis

task<Exec>("demoOption2RedisComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "docker-compose-demo-option2-redis.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=3"
    )
    dependsOn("extractUberJar")
}

task<Exec>("demoOption2RedisComposeDown") {
    commandLine("docker-compose", "-f", "docker-compose-demo-option2-redis.yaml", "down")
}

// Option 3 - RabbitMQ

task<Exec>("demoOption3RabbitComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "docker-compose-demo-option3-rabbit.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=3"
    )
    dependsOn("extractUberJar")
}

task<Exec>("demoOption3RabbitComposeDown") {
    commandLine("docker-compose", "-f", "docker-compose-demo-option3-rabbit.yaml", "down")
}

// Option 4 - Independent

task<Exec>("demoOption4IndependentMulticastComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "docker-compose-demo-option4-independent-multicast.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=3"
    )
    dependsOn("extractUberJar")
}

task<Exec>("demoOption4IndependentMulticastComposeDown") {
    commandLine("docker-compose", "-f", "docker-compose-demo-option4-independent-multicast.yaml", "down")
}

task<Exec>("demoOption4IndependentDNSComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "docker-compose-demo-option4-independent-dns.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=3"
    )
    dependsOn("extractUberJar")
}

task<Exec>("demoOption4IndependentDNSComposeDown") {
    commandLine("docker-compose", "-f", "docker-compose-demo-option4-independent-dns.yaml", "down")
}

// Scale up/down:
//  - docker-compose -f <DOCKER_COMPOSE_YAML> up --scale spring-service=<NUMBER_OF_INSTANCES> --no-recreate
