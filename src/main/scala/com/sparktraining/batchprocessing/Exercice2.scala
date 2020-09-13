package com.sparktraining.batchprocessing

import com.sparktraining.utils.SparkSessionBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Exercice2 extends SparkSessionBase {
  def main(args: Array[String]): Unit = {

    // Read data from kafka transactions topic
    val dfTransactionsString = sparkSession.read
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.36:9092")
      .option("subscribe","transactions")
      .option("startingOffsets", "earliest")
      .load()

    val transactionSchema = new StructType()
      .add("accountId", StringType)
      .add("amount",IntegerType)
      .add("timestamp",LongType)

    val dfTransactions = dfTransactionsString
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .select(from_json(col("value"),transactionSchema).as("transactions"), col("key").alias("id"))
      .select("transactions.*","id")

    dfTransactions.show()
  }
}
