package com.sparktraining.streaming.structuredstreaming.exercice3

import com.sparktraining.utils.SparkSessionBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object Exercice3 extends SparkSessionBase {
  def main(args: Array[String]): Unit = {
    val transactionHandler = new TransactionStreamHandler()

    // Read transactions from kafka
    val dfTransactionsString = transactionHandler.readStream(sparkSession)
    val schema = transactionHandler.setSchema
    // Parse json value
    val dfTransactions = transactionHandler.parseDataframe(schema, dfTransactionsString)

    // Process data & write it to a new kafka topic
    val processedDf = transactionHandler.processDataframe(dfTransactions)
    val formatedDf = transactionHandler.dfToSinkFormat(processedDf)
    transactionHandler.streamWriter(formatedDf)
  }
}
