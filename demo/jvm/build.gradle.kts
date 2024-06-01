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
    mavenCentral()
    maven {
        url = uri("https://maven.pkg.github.com/CrossStream-for-Kotlin/cross-stream-for-kotlin")
        credentials {
            username = System.getenv("GITHUB_USER")
            password = System.getenv("GITHUB_TOKEN_WITH_PACKAGE_PERMISSIONS")
        }
    }
}

dependencies {
    implementation("pt.isel.leic.cs4k:cs4k:0.0.1") // Substitua pela versão correta da sua biblioteca
}



dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.apache.tomcat.embed:tomcat-embed-core:10.1.17")
    implementation("org.apache.tomcat.embed:tomcat-embed-websocket:10.1.17")
    implementation("org.apache.tomcat.embed:tomcat-embed-el:10.1.17")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")
    implementation(project(mapOf("path" to ":")))
    implementation(project(mapOf("path" to ":")))
    implementation(project(mapOf("path" to ":")))
    implementation(project(mapOf("path" to ":")))

    testImplementation(kotlin("test"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webflux")

    // For Library
    implementation("pt.isel.leic.cs4k:cs4k:0.0.1")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}

/**
 * PostgreSQl DB related tasks.
 */
task<Exec>("postgresUp") {
    commandLine("docker-compose", "-f", "docker-compose-demo-option1.yaml", "up", "-d", "--build", "postgres")
}

task<Exec>("postgresWait") {
    commandLine("docker", "exec", "postgres", "/app/bin/wait-for-postgres.sh", "localhost")
    dependsOn("postgresUp")
}

task<Exec>("postgresDown") {
    commandLine("docker-compose", "-f", "docker-compose-demo-option1.yaml", "down")
}

/**
 * Redis related tasks
 */
task<Exec>("redisUp") {
    commandLine(
        "docker-compose",
        "-f",
        "docker-compose-demo-option2-redis.yaml",
        "up",
        "-d",
        "--build",
        "redis"
    )
}

task<Exec>("redisDown") {
    commandLine("docker-compose", "-f", "docker-compose-demo-option2-redis.yaml", "down")
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

// Option 1

task<Exec>("demoOption1ComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "docker-compose-demo-option1.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=3"
    )
    dependsOn("extractUberJar")
}

task<Exec>("demoOption1ComposeDown") {
    commandLine("docker-compose", "-f", "docker-compose-demo-option1.yaml", "down")
}

// Option 2

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

task<Exec>("demoOption2RabbitComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "docker-compose-demo-option2-rabbit.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=3"
    )
    dependsOn("extractUberJar")
}

task<Exec>("demoOption2RabbitComposeDown") {
    commandLine("docker-compose", "-f", "docker-compose-demo-option2-rabbit.yaml", "down")
}

// Option 3

task<Exec>("demoOption3ComposeUp") {
    commandLine(
        "docker-compose",
        "-f",
        "docker-compose-demo-option3.yaml",
        "up",
        "--build",
        "--force-recreate",
        "--scale",
        "spring-service=3"
    )
    dependsOn("extractUberJar")
}

task<Exec>("demoOption3ComposeDown") {
    commandLine("docker-compose", "-f", "docker-compose-demo-option3.yaml", "down")
}

// Scale up/down:
//  - docker-compose -f <DOCKER_COMPOSE_YAML> up --scale spring-service=<NUMBER_OF_INSTANCES> --no-recreate
