package com.sparktraining.streaming.structuredstreaming.exercice6

import com.sparktraining.streaming.structuredstreaming.StreamingHandler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class LogsStreamHandler extends StreamingHandler {
  /**
   * Read transactions from kafka
   * @param sparkSession
   * @return
   */
  override def readStream(sparkSession: SparkSession): DataFrame = {
    sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", " 192.168.1.36:9092")
      .option("subscribe", "logs3")
      .option("startingOffsets", "latest")
      .load()
  }

  /**
   * Split log column into to extract
   * log-level, time, class & content information
   * @param dataFrame df containing only log columns
   * @return proccessed df with multiple columns
   */
  override def processDataframe(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("splited", split(col("value"),pattern="  "))
      .withColumn("level", col("splited").getItem(0))
      .withColumn("time",col("splited").getItem(1))
      .withColumn("class", col("splited").getItem(2))
      .withColumn("content", col("splited").getItem(4))
      .select("level","time","class","content")
  }

  /**
   * Write processed stream to kafka topic
   * @param dataFrame processed logs dataframe
   */
  override def streamWriter(dataFrame: DataFrame, consoleOutput: Boolean): Unit = {
    consoleOutput match {
      case true =>
        dataFrame.writeStream
          .outputMode("append")
          .format("console")
          .start()
          .awaitTermination()

      case false =>
        dataFrame.writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "192.168.1.36:9092")
          .option("topic", "processed-logs")
          .option("checkpointLocation", "checkpoints")
          .outputMode("append")
          .start()
          .awaitTermination()
    }
  }

  override def setSchema: StructType = ???

  override def parseDataframe(schema: StructType, dataFrame: DataFrame): DataFrame = ???

  override def dfToSinkFormat(dataFrame: DataFrame): DataFrame = ???
}


