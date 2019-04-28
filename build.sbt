name := "TicketsSparkGrafana"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.2"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.2"

libraryDependencies += "org.apache.kafka" % "kafka-tools" % "0.10.0.0"

dependencyOverrides += "com.google.guava" % "guava" % "15.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.1"

libraryDependencies += "com.github.gilbertw1" %% "slack-scala-client" % "0.1.8"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.8"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.6"

libraryDependencies += "org.projectlombok" % "lombok" % "1.18.4" % "provided"

val elastic4sVersion = "6.3.8"

libraryDependencies ++= Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-http-streams" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % "test",
  "com.sksamuel.elastic4s" %% "elastic4s-embedded" % elastic4sVersion % "test"
)

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api"       % "1.7.7",
  "org.slf4j" % "jcl-over-slf4j"  % "1.7.7"
).map(_.force())

//libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-jdk14")) }

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.7" % Test
