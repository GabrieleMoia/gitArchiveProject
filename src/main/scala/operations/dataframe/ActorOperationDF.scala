package operations.dataframe

import classes.GitArchive
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.hive.HiveContext

class ActorOperationDF(sc: SparkContext) {

  val encoder = Encoders.product[GitArchive]

  val hc = new HiveContext(sc)

  import hc.implicits._


  def getActorPerHour(data: DataFrame): DataFrame = {
    val actorPerHour = data.select($"actor".as("actorId"), $"created_at").groupBy("created_at").count()
    actorPerHour
  }

  def getActorPerTypeAndHour(data: DataFrame): DataFrame = {
    val actorPerTypeAndHour = data.select($"actor.id".as("actorId"), $"created_at", $"tipo").groupBy("created_at", "tipo").count()
    actorPerTypeAndHour
  }

  def getActorPerTypeHourAndRepo(data: DataFrame): DataFrame = {
    val actorPerTypeAndHour = data.select($"actor.id".as("actorId"), $"created_at", $"tipo", $"repo.id".as("repoId")).groupBy("created_at", "tipo", "repoId").count()
    actorPerTypeAndHour
  }

  def getMinActorPerHour(data: DataFrame): DataFrame = {
    val actorPerHour = data.select($"actor.id", $"created_at").groupBy("created_at").count()
    val minActorPerHour = actorPerHour.agg(min("count"))
    minActorPerHour
  }

  def getMaxActorPerHour(data: DataFrame): DataFrame = {
    val actorPerHour = data.select($"actor.id", $"created_at").groupBy("created_at").count()
    val maxActorPerHour = actorPerHour.agg(max("count"))
    maxActorPerHour
  }

  def getMinActorPerHourAndType(data: DataFrame): DataFrame = {
    val actorPerHourAndType = data.select($"actor.id", $"created_at", $"tipo").groupBy("created_at", "tipo").count()
    val minActorPerHourAndType = actorPerHourAndType.agg(min("count"))
    minActorPerHourAndType
  }

  def getMaxActorPerHourAndType(data: DataFrame): DataFrame = {
    val actorPerHourAndType = data.select($"actor.id", $"created_at", $"tipo").groupBy("created_at", "tipo").count()
    val maxActorPerHourAndType = actorPerHourAndType.agg(max("count"))
    maxActorPerHourAndType
  }

  def getMinActorPerHourTypeAndRepo(data: DataFrame): DataFrame = {
    val actorPerHourTypeAndRepo = data.select($"actor.id", $"created_at", $"tipo", $"repo.id".as("repoId")).groupBy("created_at", "tipo", "repoId").count()
    val minActorPerHourTypeAndRepo = actorPerHourTypeAndRepo.agg(min("count"))
    minActorPerHourTypeAndRepo
  }

  def getMaxActorPerHourTypeAndRepo(data: DataFrame): DataFrame = {
    val actorPerHourTypeAndRepo = data.select($"actor.id", $"created_at", $"tipo", $"repo.id".as("repoId")).groupBy("created_at", "tipo", "repoId").count()
    val maxActorPerHourTypeAndRepo = actorPerHourTypeAndRepo.agg(max("count"))
    maxActorPerHourTypeAndRepo
  }
}
