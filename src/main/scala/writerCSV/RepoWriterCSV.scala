package writerCSV

import org.apache.spark.sql.DataFrame

object RepoWriterCSV {

  def repoDataFrameToCSV(dfRepo: DataFrame){
    dfRepo.select("id", "name", "url")
    dfRepo.coalesce(1).write.format("com.databricks.spark.csv").csv("Repo")
  }
}
