package converter

import classes.GitArchive
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

object PayloadConverter {

  def getPayloadDataSet(dataset: Dataset[GitArchive]): Dataset[Row] = {
    val actorDS = dataset.select("payload.*")
    actorDS
  }

  def getPayloadRDD(dataset: Dataset[GitArchive]): RDD[Row] = {
    val actorDS = dataset.select("payload.*").rdd
    actorDS
  }
}
