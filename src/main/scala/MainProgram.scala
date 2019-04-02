import java.io.File
import java.net.URL

import classes.{Commit, GitArchive}
import operations.dataframe.{ActorOperationDF, CommitOperationDF, EventOperationDF}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import utils.{ActorUtils, AuthorUtils, PropertiesHelperUtil, RepoUtils}

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

    //fileDownloader("http://data.githubarchive.org/2018-03-01-0.json.gz", "2018-03-01-0.json")

    val encode = Encoders.product[GitArchive]
    val jsonDFPublic2 = sqlContext.read.option("inferSchema", 20).json("2018-03-01-0.json").withColumnRenamed("public", "publico").withColumnRenamed("type", "tipo")
    val gitArchiveDs: Dataset[GitArchive] = jsonDFPublic2.as[GitArchive](encode)
    gitArchiveDs.dropDuplicates("id")

    val actor = ActorUtils.getActorDataFrame(gitArchiveDs)
    ActorUtils.actorDataFrameToCSV(actor)

    val author = AuthorUtils.getAuthorDataFrame(gitArchiveDs)
    AuthorUtils.authorDataFrameToCSV(author)

    val types = RepoUtils.getTypes(gitArchiveDs).distinct()
    RepoUtils.typeToCSV(types)

    val repo = RepoUtils.getRepoDataFrame(gitArchiveDs)
    RepoUtils.repoDataFrameToCSV(repo)

    val actorCount = ActorUtils.actorDataFrameCount(actor)
    println("acotor count " + actorCount)

    val repoCount = RepoUtils.repoDataFrameCount(repo)
    println("repo count " + repoCount)
    val eventOperations = new EventOperationDF(sc)
    val actorOperation = new ActorOperationDF(sc)
    val commitOperation = new CommitOperationDF(sc)

    val de = eventOperations.getEventPerActor(gitArchiveDs.toDF())
    de.show()

    val commit = commitOperation.getCommitPerActor(gitArchiveDs.toDF())
    commit.show()

  }

  def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }
}
