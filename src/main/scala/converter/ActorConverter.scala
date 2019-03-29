package converter

import classes.{Actor, GitArchive}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object ActorConverter {

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
}
