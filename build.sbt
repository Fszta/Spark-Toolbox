name := "Spark-training"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1",
  "com.typesafe.play" %% "play-json" % "2.9.0"
)
