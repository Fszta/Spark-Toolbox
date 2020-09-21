package com.sparktraining.streaming.structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

trait StreamingHandler {
  def readStream(sparkSession: SparkSession): DataFrame

  def setSchema: StructType

  def parseDataframe(schema: StructType, dataFrame: DataFrame): DataFrame

  def processDataframe(dataFrame: DataFrame): DataFrame

  def dfToSinkFormat(dataFrame: DataFrame): DataFrame

  def streamWriter(dataFrame: DataFrame): Unit
}
