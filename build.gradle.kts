repositories {
  mavenCentral()
}

plugins {
  java
  application
  idea
}

dependencies {
  implementation("org.duckdb:duckdb_jdbc:0.8.0")

  implementation("org.apache.arrow:arrow-vector:12.0.0")
  implementation("org.apache.arrow:arrow-c-data:12.0.0")
  implementation("org.apache.arrow:arrow-jdbc:12.0.0")
  implementation("org.apache.arrow:flight-grpc:12.0.0")
}

application {
  mainClass.set("Repro")
}