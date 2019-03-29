package writerCSV

import org.apache.spark.sql.DataFrame

object ActorWriterCSV {

  def actorDataFrameToCSV(dfActor: DataFrame){
    dfActor.select("id", "login", "display_login", "gravatar_id", "url", "avatar_url")
    dfActor.coalesce(1).write.format("com.databricks.spark.csv").csv("actor")
  }
}
