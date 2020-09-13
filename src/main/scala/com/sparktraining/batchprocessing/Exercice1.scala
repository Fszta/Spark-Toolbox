package com.sparktraining.batchprocessing

import com.sparktraining.utils.SparkSessionBase
import org.apache.spark.sql.functions._
import com.sparktraining.utils.DataGenerator.generateTransactions

object Exercice1 extends SparkSessionBase {
  def main(args: Array[String]): Unit = {
    val rddTransactions = sparkSession.sparkContext.parallelize(generateTransactions(10))

    val df = sparkSession.createDataFrame(rddTransactions)
      .select(to_json(struct(col("accountId"),col("amount"),col("timestamp")))
      .alias("value"), col("id").alias("key"))

    df.show()

    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.36:9092")
      .option("topic","transactions")
      .save()
  }
}
