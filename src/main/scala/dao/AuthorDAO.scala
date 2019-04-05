package dao

import org.apache.spark.sql.{DataFrame, SaveMode}
import utils.PropertiesHelperUtil

class AuthorDAO {
  def insertAuthor(author: DataFrame) = {
    val helper = new DBHelper()
    val connectionProperties = helper.connection()
    val applicationProperties = new PropertiesHelperUtil().getApplicationProperties()
    author.write.mode(SaveMode.Append).jdbc(applicationProperties.getProperty("jdbc.url"), "author", connectionProperties)
  }
}
