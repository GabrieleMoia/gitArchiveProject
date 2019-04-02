package operations.dataframe

import classes.GitArchive
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.hive.HiveContext
import utils.PropertiesHelperUtil

class CommitOperationDF(sc: SparkContext) {

  val encoder = Encoders.product[GitArchive]

  val hc = new HiveContext(sc)

  import hc.implicits._

  def getCommit(data: DataFrame): Long = {
    val commit = data.select($"payload.commits").count()
    commit
  }

  def getCommitPerActor(data: DataFrame): DataFrame = {
    val csvProperties = new PropertiesHelperUtil().getCSVProperties()
    val commitPerActor = data.select($"payload.commits", $"actor.id".as("actorId")).groupBy("actorId").count()
    commitPerActor.select("actorId", "count")
    commitPerActor.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").csv(csvProperties.getProperty("calculate.csv"))

    commitPerActor
  }

  def getCommitPerActorAndType(data: DataFrame): DataFrame = {
    val actorAndType = data.select($"payload.commits", $"actor.id".as("actorId"), $"tipo")
    val commitPerActorAndType = actorAndType.groupBy("tipo", "actorId").count()
    commitPerActorAndType
  }

  def getCommitPerActorTypeAndEvent(data: DataFrame): DataFrame = {
    val actorTypeAndEvent = data.select($"payload.commits", $"tipo", $"actor.id".as("actorId"), $"id".as("eventId"))
    val commitPerActorTypeAndEvent = actorTypeAndEvent.groupBy("tipo", "actorId", "eventId").count()
    commitPerActorTypeAndEvent
  }

  def getEventPerActorTypeAndHour(data: DataFrame): DataFrame = {
    val actorTypeAndHour = data.select($"id", $"tipo", $"created_at", $"actor.id".as("actorId"))
    val commitPerActorTypeAndHour = actorTypeAndHour.groupBy("tipo", "actorId", "created_at").count()
    commitPerActorTypeAndHour
  }

  def getMinCommitPerHour(data: DataFrame): DataFrame = {
    val commitPerHour = data.select($"payload.commits", $"created_at").groupBy("created_at").count()
    val minCommitPerHour = commitPerHour.agg(min("count"))
    minCommitPerHour
  }

  def getMaxCommitPerHour(data: DataFrame): DataFrame = {
    val commitPerHour = data.select($"payload.commits", $"created_at").groupBy("created_at").count()
    val maxCommitPerHour = commitPerHour.agg(max("count"))
    maxCommitPerHour
  }
}
