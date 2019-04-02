package utils

import classes._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

object AuthorUtils {

  val encode = Encoders.product[Payload]
  val encodeCommit = Encoders.product[Commits]
  val encodeAuthor = Encoders.product[Author]

  def getAuthorRDD(dataset: Dataset[GitArchive]): RDD[Author] = {
    val authorExploded = dataset.select(explode(col("payload.commits.author")).as("author"))
    val author = authorExploded.select("author.*").as[Author](encodeAuthor).rdd
    author
  }

  def getAuthorPairRDD(dataset: Dataset[GitArchive]): RDD[(String, Author)] = {
    val authorExploded = dataset.select(explode(col("payload.commits.author")).as("author"))
    val authorRDD = authorExploded.select("author.*").as[Author](encodeAuthor).rdd
    val authorPairRDD: RDD[(String, Author)] = authorRDD.map(x => (x.name, x))
    authorPairRDD
  }

  def getAuthorDataFrame(dataset: Dataset[GitArchive]): DataFrame = {
    val authorExploded = dataset.select(explode(col("payload.commits.author")).as("author"))
    val authorDataFrame = authorExploded.select("author.*").as[Author](encodeAuthor).toDF()
    authorDataFrame
  }

  def getAuthorDataSet(dataset: Dataset[GitArchive]): Dataset[Author] = {
    val authorExploded = dataset.select(explode(col("payload.commits.author")).as("author"))
    val authorDataSet = authorExploded.select("author.*").as[Author](encodeAuthor)
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
