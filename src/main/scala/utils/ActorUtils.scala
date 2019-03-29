package utils

import classes.{Actor, GitArchive}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

object ActorUtils {

  val encode = Encoders.product[Actor]

  def getActorRDD(dataset: Dataset[GitArchive]): RDD[Actor] = {

    val actorRDD = dataset.select("actor.*").dropDuplicates("id").as[Actor](encode).rdd
    actorRDD
  }

  def getActorPairRDD(dataset: Dataset[GitArchive]): RDD[(BigInt, Actor)] = {
    val actorRDD = dataset.select("actor.*").dropDuplicates("id").as[Actor](encode).rdd
    val actorPairRDD : RDD[(BigInt, Actor)] = actorRDD.map(x => (x.id, x))
    actorPairRDD
  }

  def getActorDataFrame(dataset: Dataset[GitArchive]): DataFrame = {
    val actorDataFrame = dataset.select("actor.*").dropDuplicates("id").as[Actor](encode).toDF()
    actorDataFrame
  }

  def getActorDataSet(dataset: Dataset[GitArchive]): Dataset[Actor] = {

    val actorDataSet = dataset.select("actor.*").dropDuplicates("id").as[Actor](encode)
    actorDataSet
  }

  def actorDataFrameToCSV(dfActor: DataFrame){
    dfActor.select("id", "login", "display_login", "gravatar_id", "url", "avatar_url")
    dfActor.coalesce(1).write.format("com.databricks.spark.csv").csv("actor")
  }

  def actorRDDCount(rdd: RDD[Actor]): Long = {
    rdd.count()
  }

  def actorPairRDDCount(pairRdd: RDD[(BigInt, Actor)]): Long = {
    pairRdd.count()
  }

  def actorDataFrameCount(dataFrame: DataFrame): Long = {
    dataFrame.count()
  }

  def actorDataSetCount(dataSet: Dataset[Actor]): Long = {
    dataSet.count()
  }

}
