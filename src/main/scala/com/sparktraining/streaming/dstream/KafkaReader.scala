package com.sparktraining.streaming.dstream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategy, LocationStrategy}

trait KafkaReader[T] {
  def kafkaParamsMap(brokers: String, groupId: String, offset: String): Map[String, Object]

  def readStream(streamingContext: StreamingContext, locationStrategy: LocationStrategy,
             consumerStrategy: ConsumerStrategy[String,String]): InputDStream[ConsumerRecord[String, String]]

  def processStream(inputDStream: InputDStream[ConsumerRecord[String,String]])
}
