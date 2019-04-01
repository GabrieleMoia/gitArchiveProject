package utils

import classes.{GitArchive, Repo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

object RepoUtils {

  val encode = Encoders.product[Repo]

  def getRepoRDD(dataset: Dataset[GitArchive]): RDD[Repo] = {
    val repoRDD = dataset.select("repo.*").as[Repo](encode).rdd
    repoRDD
  }

  def getRepoPairRDD(dataset: Dataset[GitArchive]): RDD[(BigInt, Repo)] = {
    val repoRDD = dataset.select("repo.*").as[Repo](encode).rdd
    val repoPairRDD: RDD[(BigInt, Repo)] = repoRDD.map(x => (x.id, x))
    repoPairRDD
  }

  def getRepoDataFrame(dataset: Dataset[GitArchive]): DataFrame = {
    val repoDataFrame = dataset.select("repo.*").as[Repo](encode).toDF()
    repoDataFrame
  }

  def getRepoDataSet(dataset: Dataset[GitArchive]): Dataset[Repo] = {
    val repoDataSet = dataset.select("repo.*").as[Repo](encode)
    repoDataSet
  }

  def getTypes(dataset: Dataset[GitArchive]): DataFrame = {
    val typeDataFrame = dataset.select("tipo").toDF()
    typeDataFrame
  }

  def typeToCSV(dfTipo:DataFrame){
    val csvProperties = new PropertiesHelperUtil().getCSVProperties()
    dfTipo.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").csv(csvProperties.getProperty("type.csv"))
  }

  def repoDataFrameToCSV(dfRepo: DataFrame) {
    val csvProperties = new PropertiesHelperUtil().getCSVProperties()

    dfRepo.select("id", "name", "url")
    dfRepo.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").csv(csvProperties.getProperty("repo.csv"))
  }

  def repoRDDCount(rdd: RDD[Repo]): Long = {
    rdd.count()
  }

  def repoPairRDDCount(pairRdd: RDD[(BigInt, Repo)]): Long = {
    pairRdd.count()
  }

  def repoDataFrameCount(dataFrame: DataFrame): Long = {
    dataFrame.count()
  }

  def repoDataSetCount(dataSet: Dataset[Repo]): Long = {
    dataSet.count()
  }
}
