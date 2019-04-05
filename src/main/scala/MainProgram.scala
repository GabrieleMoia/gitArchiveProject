import java.io._
import java.net.{HttpURLConnection, URL}
import java.util.zip.GZIPInputStream

import scala.io.Source
import classes.GitArchive
import dao.{ActorDAO, AuthorDAO, RepoDAO}
import dataset.dataframe.EventOperationDF
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, _}
import org.apache.spark.{SparkConf, SparkContext}
import utils.{ActorUtils, AuthorUtils, PropertiesHelperUtil, RepoUtils}

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

    val applicationProperties = new PropertiesHelperUtil().getApplicationProperties()
    val date = applicationProperties.getProperty("date")
    val downloadLocation = applicationProperties.getProperty("downloadLocation")

    //file download
    fileDownloader("http://data.githubarchive.org/" + date + ".json.gz", date + ".json")
    extract(downloadLocation + "/" + date + ".json.gz", date + ".json")

    //json parser
    val encode = Encoders.product[GitArchive]
    val jsonDF = sqlContext.read.option("inferSchema", 20).json("2018-03-01-0.json").withColumnRenamed("public", "publico").withColumnRenamed("type", "tipo")
    val gitArchiveDs: Dataset[GitArchive] = jsonDF.as[GitArchive](encode)
    val gitArchiveRDD: RDD[GitArchive] = jsonDF.as[GitArchive](encode).rdd

    gitArchiveDs.dropDuplicates("id")

    val actorDao = new ActorDAO()
    val authorDao = new AuthorDAO()
    val repoDao = new RepoDAO()

    //write on CSV and DB
    val actor = ActorUtils.getActorDataFrame(gitArchiveDs)
    ActorUtils.actorDataFrameToCSV(actor)
    actorDao.insertActor(actor)

    val author = AuthorUtils.getAuthorDataFrame(gitArchiveDs)
    AuthorUtils.authorDataFrameToCSV(author)
    authorDao.insertAuthor(author)

    val types = RepoUtils.getTypes(gitArchiveDs).distinct()
    RepoUtils.typeToCSV(types)

    val repo = RepoUtils.getRepoDataFrame(gitArchiveDs)
    RepoUtils.repoDataFrameToCSV(repo)
    repoDao.insertRepo(repo)

    val eventOperation = new EventOperationDF(sc)
    val df = eventOperation.getEventPerActor(gitArchiveDs.toDF())
    df.show()

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
