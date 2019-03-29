package converter

import classes.GitArchive
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

object RepoConverter {

  def getRepoRDD(dataset: Dataset[GitArchive]): RDD[Row] = {
    val repoRDD = dataset.select("repo.*").rdd
    repoRDD
  }

  def getRepoPairRDD(dataset: Dataset[GitArchive]): RDD[(Int, Row)] = {
    val repoRDD = dataset.select("repo.*").rdd
    val repoPairRDD : RDD[(Int, Row)] = repoRDD.map(x => (x.getAs("id"), x))
    repoPairRDD
  }

  def getActorDataFrame(dataset: Dataset[GitArchive], sqlContext: SQLContext): DataFrame = {
    val actorDataFrame = dataset.select("repo.*").toDF()
    actorDataFrame
  }

  def getRepoDataSet(dataset: Dataset[GitArchive]): Dataset[Row] = {
    val repoDataSet = dataset.select("repo.*")
    repoDataSet
  }


}
