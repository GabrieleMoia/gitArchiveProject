import java.io._
import java.net.{HttpURLConnection, URL}
import java.util.zip.GZIPInputStream

import scala.io.Source
import classes.{Commit, GitArchive}
import operations.dataframe.{ActorOperationDF, CommitOperationDF, EventOperationDF}
import operations.dataset.{ActorOperationDS, CommitOperationDS, EventOperationDS}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.glassfish.jersey.internal.util.PropertiesHelper
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

    val applicationProperties = new PropertiesHelperUtil().getApplicationProperties()
    val date = applicationProperties.getProperty("date")
    val downloadLocation = applicationProperties.getProperty("downloadLocation")

    //file download
    fileDownloader("http://data.githubarchive.org/" + date + ".json.gz", date + ".json")
    extract(downloadLocation + "/" + date + ".json.gz", date + ".json")

    //json parser
    val encode = Encoders.product[GitArchive]
    val jsonDF = sqlContext.read.option("inferSchema", 20).json(date + ".json").withColumnRenamed("public", "publico").withColumnRenamed("type", "tipo")
    val gitArchiveDs: Dataset[GitArchive] = jsonDF.as[GitArchive](encode)
    gitArchiveDs.dropDuplicates("id")

    //write on CSV
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
  }

  def fileDownloader(indirizzo: String, filename: String) = {
    val url = new URL(indirizzo)
    val newUrl = url.openConnection().getHeaderField("Location")
    val filename = getFilenameFromURL(newUrl)
    val connection = new URL(newUrl).openConnection()
    connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11")
    connection.connect()
    val inputStream = connection.getInputStream
    val applicationProperties = new PropertiesHelperUtil().getApplicationProperties()
    FileUtils.copyInputStreamToFile(inputStream, new File(applicationProperties.getProperty("downloadLocation") + "/" + filename))
  }

  def getFilenameFromURL(urlString: String): String = {
    val splittedUrl = urlString.split("/")
    val size = splittedUrl.length - 1
    splittedUrl(size)
  }

  def extract(input: String, output: String) {
    val inputStream = new GZIPInputStream(new FileInputStream(input))
    val outputStream = new FileOutputStream(output)
    val w = new PrintWriter(new OutputStreamWriter(outputStream, "UTF-8"))
    for (line <- Source.fromInputStream(inputStream).getLines()) {
      w.write(line + "\n")
    }
    w.close()
    outputStream.close()
  }
}
