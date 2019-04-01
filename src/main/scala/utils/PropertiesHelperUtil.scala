package utils

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.Properties

import scala.util.Try

class PropertiesHelperUtil {

  class PropertiesHelper()

  def writePropertiesSparkConf() = Try {
    val prop = new Properties
    val file = new File("src/main/resources/SparkConf.properties")
    val fos = new FileOutputStream(file)

    prop.setProperty("conf.master", "local[2]")
    prop.setProperty("conf.appName", "SparkEx")
    prop.setProperty("sqlContext.master", "local[2]")
    prop.setProperty("sqlContext.appName", "SparkParser")
    prop.store(fos, "")

    fos.close()
  }

  def writePropertiesApplication() = Try {
    val prop = new Properties
    val file = new File("src/main/resources/application.properties")
    val fos = new FileOutputStream(file)

    prop.setProperty("jdbc.driver", "org.postgresql.Driver")
    prop.setProperty("jdbc.url", "jdbc:postgresql://localhost:5432/gitArchiveProject")
    prop.setProperty("jdbc.username", "postgres")
    prop.setProperty("jdbc.password", "root")
    prop.store(fos, "")

    fos.close()
  }

  def writeCSVProperties() = Try {
    val prop = new Properties
    val file = new File("src/main/resources/csv.properties")
    val fos = new FileOutputStream(file)

    prop.setProperty("repo.csv", "RepoCSV")
    prop.setProperty("author.csv", "AuthorCSV")
    prop.setProperty("actor.csv", "ActorCSV")
    prop.setProperty("payload.csv", "PayloadCSV")
    prop.setProperty("type.csv", "TypeCSV")
    prop.setProperty("calculate.csv", "CountCSV")
    prop.store(fos, "")
    fos.close()
  }

  def getSparkConfProperties(): Properties = {
    val sparkProperties = new Properties
    val in = new FileInputStream("src/main/resources/SparkConf.properties")
    sparkProperties.load(in)
    return sparkProperties
  }

  def getApplicationProperties(): Properties = {
    val applicationProperties = new Properties
    val in = new FileInputStream("src/main/resources/application.properties")
    applicationProperties.load(in)
    return applicationProperties
  }

  def getCSVProperties(): Properties = {
    val CSVProperties = new Properties
    val in = new FileInputStream("src/main/resources/csv.properties")
    CSVProperties.load(in)
    return CSVProperties
  }
}
