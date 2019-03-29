package converter

import classes.{GitArchive, Payload}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object PayloadConverter {

  val encode = Encoders.product[Payload]

  def getPayloadRDD(dataset: Dataset[GitArchive]): RDD[Payload] = {
    val payloadRDD = dataset.select("payload.*").as[Payload](encode).rdd
    payloadRDD
  }

  def getPayloaPairRDD(dataset: Dataset[GitArchive]): RDD[(BigInt, Payload)] = {
    val payloadRDD = dataset.select("payload.*").as[Payload](encode).rdd
    val payloaPairdRDD : RDD[(BigInt, Payload)] = payloadRDD.map(x => (x.push_id, x))
    payloaPairdRDD
  }

  def getActorDataFrame(dataset: Dataset[GitArchive]): DataFrame = {
    val payloadDataFrame = dataset.select("payload.*").as[Payload](encode).toDF()
    payloadDataFrame
  }

  def getPayloadDataSet(dataset: Dataset[GitArchive]): Dataset[Payload] = {
    val payloadDataSet = dataset.select("payload.*").as[Payload](encode)
    payloadDataSet
  }

}
