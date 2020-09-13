package com.sparktraining.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionBase {
  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .config("spark.executor.memory", "1g")
    .config("spark.worker.memory","1g")
    .config("spark.driver.memory", "256m")
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")
}
