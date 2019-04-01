package utils

import classes._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

object AuthorUtils {

  val encode = Encoders.product[Payload]
  val encodeCommit = Encoders.product[Commits]
  val encodeAuthor = Encoders.product[Author]

  def getAuthorRDD(dataset: Dataset[GitArchive]): RDD[Payload] = {
    val authorRDD = dataset.select("payload.*").as[Payload](encode).rdd
    authorRDD
  }

  def getAuthorPairRDD(dataset: Dataset[GitArchive]): RDD[(BigInt, Payload)] = {
    val authorRDD = dataset.select("payload.*").as[Payload](encode).rdd
    val authorPairRDD: RDD[(BigInt, Payload)] = authorRDD.map(x => (x.push_id, x))
    authorPairRDD
  }

  def getAuthorDataFrame(dataset: Dataset[GitArchive]): DataFrame = {
    val payloadDataFrame = dataset.select("payload.*").as[Payload](encode).toDF()
    payloadDataFrame.show()
    val commit = payloadDataFrame.select("commits").as[Commits](encodeCommit).toDF()
    val authorDataFrame = commit.select("author.*").as[Author](encodeAuthor).toDF()
    authorDataFrame
  }

  def getAuthorDataSet(dataset: Dataset[GitArchive]): Dataset[Payload] = {
    val authorDataSet = dataset.select("payload.*").as[Payload](encode)
    authorDataSet
  }

  def authorDataFrameToCSV(dfAuthor: DataFrame) {
    val csvProperties = new PropertiesHelperUtil().getCSVProperties()

    dfAuthor.select("name", "email")
    dfAuthor.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").csv(csvProperties.getProperty("author.csv"))
  }

  def authorRDDCount(rdd: RDD[Author]): Long = {
    rdd.count()
  }

  def authorPairRDDCount(pairRdd: RDD[(BigInt, Author)]): Long = {
    pairRdd.count()
  }

  def authorDataFrameCount(dataFrame: DataFrame): Long = {
    dataFrame.count()
  }

  def authorDataSetCount(dataSet: Dataset[Author]): Long = {
    dataSet.count()
  }
}
