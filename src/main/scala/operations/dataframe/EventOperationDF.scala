package operations.dataframe

import classes.{Actor, GitArchive}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders}

class EventOperationDF(sc: SparkContext) {

  val encoder = Encoders.product[GitArchive]

  val hc = new HiveContext(sc)

  import hc.implicits._


  def getEventPerActor(data: DataFrame): DataFrame = {
    val eventPerActor = data.select($"id", $"actor.id".as("actorId")).groupBy("actorId").count()
    eventPerActor
  }

  def getEventPerHour(data: DataFrame): DataFrame = {
    val actorAndType = data.select($"id", $"created_at")
    val eventPerActorAndType = actorAndType.groupBy("created_at").count()
    eventPerActorAndType
  }

  def getEventPerActorAndType(data: DataFrame): DataFrame = {
    val actorAndType = data.select($"id", $"tipo", $"actor.id".as("actorId"))
    val eventPerActorAndType = actorAndType.groupBy("actorId", "tipo").count()
    eventPerActorAndType
  }

  def getEventPerActorTypeAndRepo(data: DataFrame): DataFrame = {
    val actorTypeAndRepo = data.select($"id", $"tipo", $"repo.id".as("repoId"), $"actor.id".as("actorId"))
    val eventPerActorTypeAndRepo = actorTypeAndRepo.groupBy("tipo", "actorId", "repoId").count()
    eventPerActorTypeAndRepo
  }

  def getEventPerActorTypeRepoAndHour(data: DataFrame): DataFrame = {
    val actorTypeRepoAndHour = data.select($"id", $"tipo", $"repo.id".as("repoId"), $"actor.id".as("actorId"), $"created_at")
    val eventPerActorTypeRepoAndHour = actorTypeRepoAndHour.groupBy("actorId", "tipo", "repoId", "created_at").count()
    eventPerActorTypeRepoAndHour
  }

  def getMaxEventPerActor(data: DataFrame): DataFrame = {
    val maxEventPerActor = getEventPerActor(data).agg(max("count"))
    maxEventPerActor
  }

  def getMinEventPerActor(data: DataFrame): DataFrame = {
    val minEventPerActor = getEventPerActor(data).agg(min("count"))
    minEventPerActor
  }

  def getMinEventPerHour(data: DataFrame): DataFrame = {
    val eventPerHour = data.select($"id", $"created_at").groupBy("created_at").count()
    val minEventPerHour = eventPerHour.agg(min("count"))
    minEventPerHour
  }

  def getMaxEventPerHour(data: DataFrame): DataFrame = {
    val eventPerHour = data.select($"id", $"created_at").groupBy("created_at").count()
    val minEventPerHour = eventPerHour.agg(max("count"))
    minEventPerHour
  }

  def getMinEventPerRepo(data: DataFrame): DataFrame = {
    val eventPerRepo = data.select($"id", $"repo.id".as("repoId")).groupBy("repoId").count()
    val minEventPerRepo = eventPerRepo.agg(min("count"))
    minEventPerRepo
  }

  def getMaxEventPerRepo(data: DataFrame): DataFrame = {
    val eventPerRepo = data.select($"id", $"repo.id".as("repoId")).groupBy("repoId").count()
    val maxEventPerRepo = eventPerRepo.agg(max("count"))
    maxEventPerRepo
  }

  def getMinEventPerActorAndHour(data: DataFrame): DataFrame = {
    val eventPerActorAndHour = data.select($"id", $"actor.id".as("actorId"), $"created_at").groupBy("actorId", "created_at").count()
    val minEventPerActorAndHour = eventPerActorAndHour.agg(min("count"))
    minEventPerActorAndHour
  }

  def getMaxEventPerActorAndHour(data: DataFrame): DataFrame = {
    val eventPerActorAndHour = data.select($"id", $"actor.id".as("actorId"), $"created_at").groupBy("actorId", "created_at").count()
    val maxEventPerActorAndHour = eventPerActorAndHour.agg(max("count"))
    maxEventPerActorAndHour
  }

  def getMinEventPerRepoAndHour(data: DataFrame): DataFrame = {
    val eventPerRepoAndHour = data.select($"id", $"repo.id".as("repoId"), $"created_at").groupBy("repoId", "created_at").count()
    val minEventPerRepoAndHour = eventPerRepoAndHour.agg(min("count"))
    minEventPerRepoAndHour
  }

  def getMaxEventPerRepoAndHour(data: DataFrame): DataFrame = {
    val eventPerRepoAndHour = data.select($"id", $"repo.id".as("repoId"), $"created_at").groupBy("repoId", "created_at").count()
    val maxEventPerRepoAndHour = eventPerRepoAndHour.agg(max("count"))
    maxEventPerRepoAndHour
  }

  def getMinEventPerRepoHourAndActor(data: DataFrame): DataFrame = {
    val eventPerRepoHourAndActor = data.select($"id", $"repo.id".as("repoId"), $"created_at", $"actor.id".as("actorId")).groupBy("repoId", "created_at", "actorId").count()
    val minEventPerRepoHourAndActor = eventPerRepoHourAndActor.agg(min("count"))
    minEventPerRepoHourAndActor
  }

  def getMaxEventPerRepoHourAndActor(data: DataFrame): DataFrame = {
    val eventPerRepoHourAndActor = data.select($"id", $"repo.id".as("repoId"), $"created_at", $"actor.id".as("actorId")).groupBy("repoId", "created_at", "actorId").count()
    val maxEventPerRepoHourAndActor = eventPerRepoHourAndActor.agg(max("count"))
    maxEventPerRepoHourAndActor
  }
}
