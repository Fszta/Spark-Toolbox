package com.sparktraining.streaming.dstream.exercice5

import com.sparktraining.streaming.dstream.KafkaReader
import com.sparktraining.utils.DataGenerator.Transaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategy, KafkaUtils, LocationStrategy}
import org.json4s.native.JsonMethods
import org.json4s._


class TransactionsReader extends KafkaReader[Transaction] {

  override def kafkaParamsMap(brokers: String, groupdId: String, offset: String): Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupdId,
    "auto.offset.reset" -> offset,
  )

  /**
   * Read stream from kafka
   *
   * @param ssc spark streaming context
   * @param locationStrategy
   * @param consumerStrategy
   * @return
   */
  override def readStream(ssc: StreamingContext,
                          locationStrategy: LocationStrategy,
                          consumerStrategy: ConsumerStrategy[String, String]
                         ): InputDStream[ConsumerRecord[String, String]] = {
    val stream = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy)

    stream
  }

  /**
   * Convert json value to transaction and
   * filter postivie transcation
   * @param inputDStream
   */
  override def processStream(inputDStream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    val transactionsJson = inputDStream.map(kafkaMessage => kafkaMessage.value())

    val transactions = transactionsJson.map {
      value =>
        implicit val formats = DefaultFormats
        JsonMethods.parse(value).extract[Transaction]
    }

    // Filter positif transactions
    transactions.filter(_.amount > 0)
    transactions.print()
  }
}
