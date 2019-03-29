package converter

import classes.{GitArchive, Repo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}

object RepoConverter {

  val encode = Encoders.product[Repo]

  def getRepoRDD(dataset: Dataset[GitArchive]): RDD[Repo] = {
    val repoRDD = dataset.select("repo.*").as[Repo](encode).rdd
    repoRDD
  }

  def getRepoPairRDD(dataset: Dataset[GitArchive]): RDD[(BigInt, Repo)] = {
    val repoRDD = dataset.select("repo.*").as[Repo](encode).rdd
    val repoPairRDD : RDD[(BigInt, Repo)] = repoRDD.map(x => (x.id, x))
    repoPairRDD
  }

  def getActorDataFrame(dataset: Dataset[GitArchive]): DataFrame = {
    val actorDataFrame = dataset.select("repo.*").as[Repo](encode).toDF()
    actorDataFrame
  }

  def getRepoDataSet(dataset: Dataset[GitArchive]): Dataset[Repo] = {
    val repoDataSet = dataset.select("repo.*").as[Repo](encode)
    repoDataSet
  }


}
