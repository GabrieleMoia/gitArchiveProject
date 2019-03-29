package utils

import classes.{GitArchive, Payload}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

object PayloadUtils {

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

  def getPayloadDataFrame(dataset: Dataset[GitArchive]): DataFrame = {
    val payloadDataFrame = dataset.select("payload.*").as[Payload](encode).toDF()
    payloadDataFrame
  }

  def getPayloadDataSet(dataset: Dataset[GitArchive]): Dataset[Payload] = {
    val payloadDataSet = dataset.select("payload.*").as[Payload](encode)
    payloadDataSet
  }

  def payloadDataFrameToCSV(dfPayload: DataFrame){
    dfPayload.select("push_id", "size", "distinct_size", "ref", "head", "before", "commits")
    dfPayload.coalesce(1).write.format("com.databricks.spark.csv").csv("payload")
  }

  def payloadRDDCount(rdd: RDD[Payload]): Long = {
    rdd.count()
  }

  def payloadPairRDDCount(pairRdd: RDD[(BigInt, Payload)]): Long = {
    pairRdd.count()
  }

  def payloadDataFrameCount(dataFrame: DataFrame): Long = {
    dataFrame.count()
  }

  def payloadDataSetCount(dataSet: Dataset[Payload]): Long = {
    dataSet.count()
  }
}
