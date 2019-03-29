package converter

import classes.{GitArchive}
import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql._

object ActorConverter {

  def getActorRDD(dataset: Dataset[GitArchive]): RDD[Row] = {
    val actorRDD = dataset.select("actor.*").dropDuplicates("id").rdd
    actorRDD
  }

  def getActorPairRDD(dataset: Dataset[GitArchive]): RDD[(Int, Row)] = {
    val actorRDD = dataset.select("actor.*").dropDuplicates("id").rdd
    val actorPairRDD : RDD[(Int, Row)] = actorRDD.map(x => (x.getAs("id"), x))
    actorPairRDD
  }

  def getActorDataFrame(dataset: Dataset[GitArchive], sqlContext: SQLContext): DataFrame = {
    val actorDataFrame = dataset.select("actor.*").dropDuplicates("id").toDF()
    actorDataFrame
  }

  def getActorDataSet(dataset: Dataset[GitArchive]): Dataset[Row] = {
    val actorDataSet = dataset.select("actor.*").dropDuplicates("id")
    actorDataSet
  }
}
