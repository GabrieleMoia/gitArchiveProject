import scala.util.Try
import java.io.{ File, FileOutputStream }
import java.util.Properties

class PropertiesHelper {

  class PropertiesHelper()

  def writePropertiesSparkConf ()  = Try {
    val prop = new Properties
    val file = new File("src/main/resources/SparkConf.properties")
    val fos = new FileOutputStream(file)

    prop.setProperty("", "")

    prop.store(fos, "")

    fos.close();
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

    fos.close();
  }

}
