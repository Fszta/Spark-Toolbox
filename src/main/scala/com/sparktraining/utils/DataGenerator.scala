package com.sparktraining.utils

import scala.util.Random

object DataGenerator {
  /**
   * Generate random transactions
   * @param nbSamples number of sample to generate
   * @return Sequence of Transaction
   */
  def generateTransactions(nbSamples: Int) : Seq[Transaction] = {
    (1 to nbSamples).map(_ => {
      Transaction(
        java.util.UUID.randomUUID().toString,
        java.util.UUID.randomUUID().toString,
        Random.nextInt(65536)-32768,
        System.currentTimeMillis() + Random.nextInt(10000) - 5000
      )
    })
  }

  case class Transaction(id: String, accountId: String, amount: Int,  timestamp: Long)
}
