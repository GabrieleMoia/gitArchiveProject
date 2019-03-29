package writerCSV

import org.apache.spark.sql.DataFrame

object payloadWriterCSV {
  def payloadDataFrameToCSV(dfPayload: DataFrame){
    dfPayload.select("push_id", "size", "distinct_size", "ref", "head", "before", "commits")
    dfPayload.coalesce(1).write.format("com.databricks.spark.csv").csv("payload")
  }
}
