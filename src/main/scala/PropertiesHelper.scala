import scala.util.Try
import java.io.{File, FileInputStream, FileOutputStream}
import java.util.Properties

class PropertiesHelper {

  class PropertiesHelper()

  def writePropertiesSparkConf ()  = Try {
    val prop = new Properties
    val file = new File("src/main/resources/SparkConf.properties")
    val fos = new FileOutputStream(file)

    prop.setProperty("conf.master", "local[2]")
    prop.setProperty("conf.appName","SparkEx")
    prop.setProperty("sqlContext.master", "local")
    prop.setProperty("sqlContext.sppName", "SparkParser")
    prop.store(fos, "")

    fos.close()
  }

  def writePropertiesApplication ()  = Try {
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

  def getSparkConfProperties () : Properties = {
    val sparkProperties = new Properties
    val in = new FileInputStream("src/main/resources/SparkConf.properties")
    sparkProperties.load(in)
    return sparkProperties
  }

  def getApplicationProperties () : Properties = {
    val applicationProperties = new Properties
    val in = new FileInputStream("src/main/resources/application.properties")
    applicationProperties.load(in)
    return applicationProperties
  }
}
