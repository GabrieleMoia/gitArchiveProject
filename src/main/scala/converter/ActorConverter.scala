package converter

import classes.GitArchive
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object ActorConverter {

  def getActorDataSet(dataset: Dataset[GitArchive]): Dataset[Row] = {
    val actorDS = dataset.select("actor.*").dropDuplicates("id")
    actorDS
  }

  def getActorRDD(dataset: Dataset[GitArchive]): RDD[Row] = {
    val actorDS = dataset.select("actor.*").dropDuplicates("id").rdd
    actorDS
  }


}
