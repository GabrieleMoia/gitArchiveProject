package operations.dataset

import classes.{Actor, GitArchive}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class ActorOperationDS(sparkSession: SparkSession) {

  import sparkSession.sqlContext.implicits._

  def getActorPerHour(data: Dataset[GitArchive]): Dataset[(String, BigInt)] = {
    val actorAndHour = data.select($"created_at", $"actor.id".as("actorId"))
    val actorPerHour = actorAndHour.groupBy("created_at").count()
    val actorPerHourDS = actorPerHour.as[(String, BigInt)]
    actorPerHourDS
  }

  def getActorPerTypeAndHour(data: Dataset[GitArchive]): Dataset[(String, String, BigInt)] = {
    val actorTypeAndHour = data.select($"created_at", $"tipo", $"actor.id".as("actorId"))
    val actorPerTypeAndHour = actorTypeAndHour.groupBy("created_at", "tipo").count()
    val actorPerTypeAndHourDS = actorPerTypeAndHour.as[(String, String, BigInt)]
    actorPerTypeAndHourDS
  }

  def getActorPerTypeHourAndRepo(data: Dataset[GitArchive]): Dataset[(String, String, BigInt, BigInt)] = {
    val actorTypeHourAndRepo = data.select($"created_at", $"tipo", $"repo.id".as("repoId"), $"actor.id".as("actorId"))
    val actorPerTypeHourAndRepo = actorTypeHourAndRepo.groupBy("tipo", "created_at", "repoId").count()
    val actorPerTypeHourAndRepoDS = actorPerTypeHourAndRepo.as[(String, String, BigInt, BigInt)]
    actorPerTypeHourAndRepoDS
  }

  def getMaxActorPerHour(data: Dataset[GitArchive]): Dataset[(String, BigInt)] = {
    val actorPerHour = getActorPerHour(data)
    val maxActorPerHour = actorPerHour.reduce((x, y) => {
      if (x._2 > y._2) {
        x
      } else y
    })
    val result = actorPerHour.filter("count = " + maxActorPerHour._2)
    result
  }

  def getMinActorPerHour(data: Dataset[GitArchive]): Dataset[(String, BigInt)] = {
    val actorPerHour = getActorPerHour(data)
    val minActorPerHour = actorPerHour.reduce((x, y) => {
      if (x._2 < y._2) {
        x
      } else y
    })
    val result = actorPerHour.filter("count = " + minActorPerHour._2)
    result
  }

  def getMaxActorPerHourAndType(data: Dataset[GitArchive]): Dataset[(String, String, BigInt)] = {
    val actorPerHourAndType = getActorPerTypeAndHour(data)
    val maxActorPerHourAndType = actorPerHourAndType.reduce((x, y) => {
      if (x._3 > y._3) {
        x
      } else y
    })
    val result = actorPerHourAndType.filter("count = " + maxActorPerHourAndType._3)
    result
  }

  def getMinActorPerHourAndType(data: Dataset[GitArchive]): Dataset[(String, String, BigInt)] = {
    val actorPerHourAndType = getActorPerTypeAndHour(data)
    val minActorPerHourAndType = actorPerHourAndType.reduce((x, y) => {
      if (x._3 < y._3) {
        x
      } else y
    })
    val result = actorPerHourAndType.filter("count = " + minActorPerHourAndType._3)
    result
  }

  def getMaxActorPerHourTypeAndRepo(data: Dataset[GitArchive]): Dataset[(String, String, BigInt, BigInt)] = {
    val actorPerHourTypeAndRepo = getActorPerTypeHourAndRepo(data)
    val maxActorPerHourTypeAndRepo = actorPerHourTypeAndRepo.reduce((x, y) => {
      if (x._4 > y._4) {
        x
      } else y
    })
    val result = actorPerHourTypeAndRepo.filter("count = " + maxActorPerHourTypeAndRepo._4)
    result
  }

  def getMinActorPerHourTypeAndRepo(data: Dataset[GitArchive]): Dataset[(String, String, BigInt, BigInt)] = {
    val actorPerHourTypeAndRepo = getActorPerTypeHourAndRepo(data)
    val minActorPerHourTypeAndRepo = actorPerHourTypeAndRepo.reduce((x, y) => {
      if (x._4 < y._4) {
        x
      } else y
    })
    val result = actorPerHourTypeAndRepo.filter("count = " + minActorPerHourTypeAndRepo._4)
    result
  }
}
