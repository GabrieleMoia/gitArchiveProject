package dao

import java.util.Properties

import utils.PropertiesHelperUtil

class DBHelper {

  def connection(): Properties = {
    val connectionProperties = new Properties()
    val applicationProperties = new PropertiesHelperUtil().getApplicationProperties()
    connectionProperties.put("user", applicationProperties.getProperty("jdbc.username"))
    connectionProperties.put("password", applicationProperties.getProperty("jdbc.password"))
    connectionProperties
  }
}
