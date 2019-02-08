import java.io.File
import java.net.URL

import classes.Actor
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process._

object download {
  def main(args: Array[String]): Unit = {
    val sparkConfProperties = new PropertiesHelper().getSparkConfProperties()
    val conf = new SparkConf().setMaster(sparkConfProperties.getProperty("conf.master"))
      .setAppName(sparkConfProperties.getProperty("conf.appName"))

    val sc = new SparkContext(conf)
    val sqlContext = new SparkSession.Builder().master(sparkConfProperties.getProperty("sqlContext.master"))
      .appName(sparkConfProperties.getProperty("sqlContext.appName"))
      .config("spark.some.config.option", sparkConfProperties.getProperty("sqlContext.configOption"))
      .getOrCreate()
    import sqlContext.implicits._

    fileDownloader("https://srv-file1.gofile.io/download/e5xxGs/57b9b04d902588405c3d4c6022e151ee/2018-03-01-0.json.gz", "righe.gz")

    val schema_actor = ScalaReflection.schemaFor[Actor].dataType.asInstanceOf[StructType]
    schema_actor.printTreeString()
    val rdd = sc.textFile("righe.gz")

    val json_git = sqlContext.read.json("righe.gz")
    json_git.show()

    val actor_field = json_git.select("actor")
    val actor_json = actor_field.toJSON

    val record = sqlContext.read.schema(schema_actor).json(actor_field.toJSON)

    record.show()
  }

  def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }
}
