package converter

import classes.GitArchive
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

object RepoConverter {

  def getRepoDataSet(dataset: Dataset[GitArchive]): Dataset[Row] = {
    val actorDS = dataset.select("repo.*")
    actorDS
  }

  def getRepoRDD(dataset: Dataset[GitArchive]): RDD[Row] = {
    val actorDS = dataset.select("repo.*").rdd
    actorDS
  }
}
