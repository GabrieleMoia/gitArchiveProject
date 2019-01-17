import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

class SuperParser {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    val conf: SparkConf = new SparkConf().setAppName("Hello").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)

  }
}
