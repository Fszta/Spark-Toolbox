package com.sparktraining.streaming.dstream.exercice4

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Exercice4  {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("StreamingTraining")
      .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(10))

    // Stream from text file
    val lines = ssc.textFileStream("/data")

    // Extract persons firstname
    val firstnames = lines.flatMap{ line =>
      val persons = line.split(" ")
      persons.map(_.split(",")(0))
    }
    val pairs = firstnames.map((_,1))
    val namesCount = pairs.reduceByKey(_+_)
    namesCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
