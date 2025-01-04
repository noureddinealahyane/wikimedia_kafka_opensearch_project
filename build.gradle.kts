plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation ("org.apache.kafka:kafka-clients:3.7.0")
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("org.slf4j:slf4j-simple:2.0.9")
    implementation("org.opensearch.client:opensearch-rest-high-level-client:2.18.0")
    implementation("org.apache.httpcomponents.client5:httpclient5:5.4")

    //implementation("org.opensearch.client:opensearch-rest-high-level-client:2.8.0")
}

tasks.test {
    useJUnitPlatform()
}