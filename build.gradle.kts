import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.4.21"
    application
}

group = "dk.powersystem"
version = "1.0.0"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/") {
        content {
            includeGroupAndSubgroups("io.confluent")
            includeGroup("org.apache.kafka")
        }
    }
}

val kafkaVersion = "4.3.1"

dependencies {
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("io.confluent:kafka-streams-avro-serde:8.3.2") {
        // Keep kafka-clients aligned with Kafka Streams version.
        exclude(group = "org.apache.kafka", module = "kafka-clients")
    }
    implementation("org.apache.avro:avro:1.12.2")
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.22.3"))
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("org.slf4j:slf4j-api:2.0.20")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.20")

    testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
    testImplementation("org.assertj:assertj-core:3.27.7")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.1.3")
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

kotlin {
    compilerOptions.jvmTarget = JvmTarget.JVM_21
}

application {
    mainClass = "dk.powersystem.MainKt"
}

tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }
}

tasks.register<Test>("integrationTest") {
    group = "verification"
    description = "Runs integration tests against local Kafka and Schema Registry"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform {
        includeTags("integration")
    }
    shouldRunAfter(tasks.test)
}

tasks.withType<Test>().configureEach {
    val testTemporaryDirectory = layout.buildDirectory.dir("tmp/$name").get().asFile
    systemProperty("java.io.tmpdir", testTemporaryDirectory.absolutePath)
    doFirst {
        testTemporaryDirectory.mkdirs()
    }
}
