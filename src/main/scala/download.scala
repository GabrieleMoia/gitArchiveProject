import java.io.File
import java.net.URL
import sys.process._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object download {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("SparkEx")

    val sc = new SparkContext(conf)
    val sqlContext = new SparkSession.Builder().master("local")
      .appName("SparkParser")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import sqlContext.implicits._

    fileDownloader("https://srv-file1.gofile.io/download/e5xxGs/57b9b04d902588405c3d4c6022e151ee/2018-03-01-0.json.gz", "righe.gz")


    val df = sqlContext.read.json("righe.gz")
    df.show()

    //df.collect().foreach(new GitArchive($"id", $"type", $"actor", $"repo",$"payload", $"public",$"created_at"))

  }

  def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }
}
