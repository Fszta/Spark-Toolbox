package com.sparktraining.streaming.structuredstreaming.exercice3

import com.sparktraining.streaming.structuredstreaming.StreamingHandler
import org.apache.spark.sql.functions.{col, from_json, struct, to_json, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class TransactionStreamHandler extends StreamingHandler {
  override def readStream(sparkSession: SparkSession): DataFrame = {
    sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.36:9092")
      .option("subscribe", "trans")
      .option("startingOffsets", "latest")
      .load()
  }

  override def setSchema: StructType = {
    val transactionSchema = new StructType()
      .add("id", StringType)
      .add("accountId", StringType)
      .add("amount", IntegerType)
      .add("timestamp", LongType)
    transactionSchema
  }

  override def parseDataframe(schema: StructType, dataFrame: DataFrame): DataFrame = {
    val dfTransactions = dataFrame
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("transactions"))
      .select("transactions.*")
    dfTransactions
  }

  override def processDataframe(dataFrame: DataFrame): DataFrame = {
    val processedDf = dataFrame.withColumn("transactionType",
      when(col("amount") < 0, "debit").otherwise("credit")
    )
    processedDf
  }

  override def dfToSinkFormat(dataFrame: DataFrame): DataFrame = {
    dataFrame.select(
      to_json(struct("id", "accountId", "amount", "transactionType", "timestamp")).as("value")
    )
  }

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
