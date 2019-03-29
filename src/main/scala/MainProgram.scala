import java.io.File
import java.net.URL

import classes.GitArchive
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import utils.ActorUtils

import scala.sys.process._

object MainProgram {
  def main(args: Array[String]): Unit = {
    val sparkConfProperties = new PropertiesHelperUtil().getSparkConfProperties()
    val conf = new SparkConf().setMaster(sparkConfProperties.getProperty("conf.master"))
      .setAppName(sparkConfProperties.getProperty("conf.appName"))
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder()
      .master(sparkConfProperties.getProperty("sqlContext.master"))
      .appName(sparkConfProperties.getProperty("sqlContext.appName"))
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import sqlContext.implicits._

    fileDownloader("http://data.githubarchive.org/2018-03-01-0.json.gz", "")

    val encode = Encoders.product[GitArchive]
    val jsonDFPublic2 = sqlContext.read.option("inferSchema", 20).json("2018-03-01-0.json").withColumnRenamed("public", "publico")
    val gitArchiveDs : Dataset[GitArchive] = jsonDFPublic2.as[GitArchive](encode)
    gitArchiveDs.dropDuplicates("id")


    val a1 = ActorUtils.getActorDataSet(gitArchiveDs)
    val a2 = ActorUtils.getActorDataFrame(gitArchiveDs)
    val a3 = ActorUtils.getActorPairRDD(gitArchiveDs)


    val a4 = ActorUtils.getActorRDD(gitArchiveDs).count()


  }

  def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }
}
