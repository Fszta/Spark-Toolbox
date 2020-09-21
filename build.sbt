name := "Spark-training"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.1",
  "com.typesafe.play" %% "play-json" % "2.9.0",
  "org.json4s" %% "json4s-jackson" % "3.6.9",
  "org.json4s" %% "json4s-native" % "3.6.9"
)
