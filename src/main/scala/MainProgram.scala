import java.io.File
import java.net.URL

import classes.{Actor, GitArchive}
import converter.{ActorConverter, PayloadConverter, RepoConverter}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

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
    gitArchiveDs.show()



    val a = ActorConverter.getActorDataSet(gitArchiveDs)
    a.show()

    val b = RepoConverter.getRepoDataSet(gitArchiveDs)
    b.show()

    val c = PayloadConverter.getPayloadDataSet(gitArchiveDs)
    c.show()
  }

  def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }
}
