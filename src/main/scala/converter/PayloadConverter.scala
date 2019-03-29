package converter

import classes.GitArchive
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

object PayloadConverter {

  def getPayloadRDD(dataset: Dataset[GitArchive]): RDD[Row] = {
    val payloadRDD = dataset.select("payload.*").rdd
    payloadRDD
  }

  def getPayloaPairRDD(dataset: Dataset[GitArchive]): RDD[(Int, Row)] = {
    val payloadRDD = dataset.select("payload.*").rdd
    val payloaPairdRDD : RDD[(Int, Row)] = payloadRDD.map(x => (x.getAs("push_id"), x))
    payloaPairdRDD
  }

  def getActorDataFrame(dataset: Dataset[GitArchive], sqlContext: SQLContext): DataFrame = {
    val payloadDataFrame = dataset.select("payload.*").toDF()
    payloadDataFrame
  }

  def getPayloadDataSet(dataset: Dataset[GitArchive]): Dataset[Row] = {
    val payloadDataSet = dataset.select("payload.*")
    payloadDataSet
  }

}
