package com.sparktraining.streaming.structuredstreaming.exercice6

import com.sparktraining.utils.SparkSessionBase

object Exercice6 extends SparkSessionBase {
  def main(args: Array[String]): Unit = {

    val logsStreamHandler = new LogsStreamHandler()

    // Read logs from kafka topic
    val dflogs = logsStreamHandler.readStream(sparkSession)

    // Split logs columns to get level time class & log content
    val processedDfLogs = logsStreamHandler.processDataframe(dflogs)

    // Write stream to sink (kafka / console)
    logsStreamHandler.streamWriter(processedDfLogs, true)
  }
}