package com.sparktraining.streaming

import com.sparktraining.utils.SparkSessionBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Exercice3 extends SparkSessionBase {
  def main(args: Array[String]): Unit = {
    // Read data from kafka transactions topic
    val dfTransactionsString = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.36:9092")
      .option("subscribe","transactions")
      .option("startingOffsets", "earliest")
      .load()

    val transactionSchema = new StructType()
      .add("accountId", StringType)
      .add("amount", IntegerType)
      .add("timestamp", LongType)

    val dfTransactions = parseDataFromKafka(transactionSchema, dfTransactionsString)
    val processedDf = processDataframe(dfTransactions)
    val dfKafkaFormat = formatDfToKafka(processedDf)

    dfKafkaFormat.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.36:9092")
      .option("topic","processed-transactions")
      .option("checkpointLocation","checkpoints")
      .outputMode("append")
      .start()
      .awaitTermination()

    /* dfKafkaFormat.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination() */

  }

  /**
   *
   * @param schema transaction schema
   * @param transactionDfStr dataframe from kafka
   * @return dataframe
   */
  def parseDataFromKafka(schema: StructType,transactionDfStr : DataFrame) : DataFrame = {
    val dfTransactions = transactionDfStr
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .select(from_json(col("value"),schema).as("transactions"), col("key").alias("id"))
      .select("transactions.*","id")

    dfTransactions
  }

  /**
   * Add transaction type column
   * @param dataFrame
   * @return
   */
  def processDataframe(dataFrame: DataFrame) : DataFrame = {
    val processedDf = dataFrame.withColumn("transactionType",
      when(col("amount") < 0,"debit").otherwise("credit")
    )
    processedDf
  }

  def formatDfToKafka(dataFrame: DataFrame) : DataFrame = {
    dataFrame.select(
      col("id").as("key"),
      to_json(struct("accountId","amount","transactionType","timestamp")).as("value")
    )
  }
}
