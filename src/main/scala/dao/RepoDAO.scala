package dao

import org.apache.spark.sql.{DataFrame, SaveMode}
import utils.PropertiesHelperUtil

class RepoDAO {
  def insertRepo(repo: DataFrame) = {
    val helper = new DBHelper()
    val connectionProperties = helper.connection()
    val applicationProperties = new PropertiesHelperUtil().getApplicationProperties()
    repo.write.mode(SaveMode.Append).jdbc(applicationProperties.getProperty("jdbc.url"), "repo", connectionProperties)
  }
}
