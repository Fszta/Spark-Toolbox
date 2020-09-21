package com.sparktraining.streaming.dstream.exercice5

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Exercice5 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("StreamingTraining")
      .setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(10))
    val topics = Array("transactions")
    val transactionsReader = new TransactionsReader()
    val kafkaParameters = transactionsReader.kafkaParamsMap(
      "192.168.1.36:9092",
      "exercice5",
      "latest"
    )

    // Read stream and process data
    val transactionStream = transactionsReader.readStream(ssc, PreferConsistent,Subscribe[String, String](topics, kafkaParameters))
    transactionsReader.processStream(transactionStream)

    ssc.start()
    ssc.awaitTermination()
  }
}
