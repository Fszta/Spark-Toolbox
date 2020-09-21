package com.sparktraining.streaming.structuredstreaming.exercice3

import com.sparktraining.streaming.structuredstreaming.StreamingHandler
import org.apache.spark.sql.functions.{col, from_json, struct, to_json, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class TransactionStreamHandler extends StreamingHandler {
  /**
   * Read transactions from kafka
   * @param sparkSession
   * @return
   */
  override def readStream(sparkSession: SparkSession): DataFrame = {
    sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.36:9092")
      .option("subscribe", "transactions")
      .option("startingOffsets", "latest")
      .load()
  }

  /**
   * Set transaction schema
   * @return transaction schema
   */
  override def setSchema: StructType = {
    val transactionSchema = new StructType()
      .add("id", StringType)
      .add("accountId", StringType)
      .add("amount", IntegerType)
      .add("timestamp", LongType)
    transactionSchema
  }

  /**
   * Parse json value column
   * @param schema transactions schema
   * @param dataFrame df containing col json
   * @return
   */
  override def parseDataframe(schema: StructType, dataFrame: DataFrame): DataFrame = {
    val dfTransactions = dataFrame
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("transactions"))
      .select("transactions.*")
    dfTransactions
  }

  /**
   * Basic processing on df
   * @param dataFrame input df
   * @return processed df
   */
  override def processDataframe(dataFrame: DataFrame): DataFrame = {
    val processedDf = dataFrame
      .withColumn("date",to_date(col("timestamp"),"yyyy-MM-dd"))
      .withColumn("transactionType",
        when(col("amount") < 0, "debit").otherwise("credit")
    )
    processedDf
  }


  /**
   * Format df to kafka format
   * With column value containings json
   * @param dataFrame
   * @return df kafka format
   */
  override def dfToSinkFormat(dataFrame: DataFrame): DataFrame = {
    dataFrame.select(
      to_json(struct("id", "accountId", "amount", "transactionType", "timestamp")).as("value")
    )
  }

  /**
   * Write stream to kafka topic
   * @param dataFrame
   */
  override def streamWriter(dataFrame: DataFrame): Unit = {
    dataFrame.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.36:9092")
      .option("topic", "processed-transaction")
      .option("checkpointLocation", "checkpoints")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
