package dao

import java.util.Properties

import org.apache.log4j.receivers.db.DBHelper
import org.apache.spark.sql.DataFrame
import utils.PropertiesHelperUtil

class ActorDAO {

  def insertActor(actor: DataFrame) = {
    val helper = new DBHelper()
    val connectionProperties = helper.connection()
    val applicationProperties = new PropertiesHelperUtil().getApplicationProperties()
    actor.write.jdbc(applicationProperties.getProperty("jdbc.url"), "actor", connectionProperties)
  }

}
