package operations.dataset

import classes.GitArchive
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import utils.PropertiesHelperUtil

class CommitOperationDS(sparkSession: SparkSession) {

  import sparkSession.sqlContext.implicits._

  def getCommit(data: Dataset[GitArchive]): Long = {
    val commit = data.select($"payload.commits").count()
    commit
  }

  def getCommitPerActor(data: Dataset[GitArchive]): Dataset[(String, BigInt)] = {
    val commitAndActor = data.select($"payload.commits", $"actor.id".as("actorId"))
    val commitPerActor = commitAndActor.groupBy("actorId").count()
    val actorPerHourDS = commitPerActor.as[(String, BigInt)]
    actorPerHourDS
  }

  def getCommitPerHour(data: Dataset[GitArchive]): Dataset[(String, BigInt)] = {
    val commitAndHour = data.select($"payload.commits", $"created_at")
    val commitPerHour = commitAndHour.groupBy("created_at").count()
    val commitPerHourDS = commitPerHour.as[(String, BigInt)]
    commitPerHourDS
  }

  def getCommitPerActorAndType(data: Dataset[GitArchive]): Dataset[(String, BigInt, BigInt)] = {
    val commitActorAndType = data.select($"payload.commits", $"tipo", $"actor.id".as("actorId"))
    val commitPerActorAndType = commitActorAndType.groupBy("tipo", "actorId").count()
    val commitPerActorAndTypeDS = commitPerActorAndType.as[(String, BigInt, BigInt)]
    commitPerActorAndTypeDS
  }

  def getCommitPerActorTypeAndEvent(data: Dataset[GitArchive]): Dataset[(String, BigInt, String, BigInt)] = {
    val actorTypeAndEvent = data.select($"payload.commits", $"tipo", $"actor.id".as("actorId"), $"id".as("eventId"))
    val commitPerActorTypeAndEvent = actorTypeAndEvent.groupBy("tipo", "actorId", "eventId").count()
    val commitPerActorTypeAndEventDS = commitPerActorTypeAndEvent.as[(String, BigInt, String, BigInt)]
    commitPerActorTypeAndEventDS
  }

  def getCommitPerActorTypeAndHour(data: Dataset[GitArchive]): Dataset[(String, BigInt, String, BigInt)] = {
    val actorTypeAndHour = data.select($"payload.commits", $"tipo", $"actor.id".as("actorId"), $"created_at")
    val commitPerActorTypeAndHour = actorTypeAndHour.groupBy("tipo", "actorId", "created_at").count()
    val commitPerActorTypeAndHourDS = commitPerActorTypeAndHour.as[(String, BigInt, String, BigInt)]
    commitPerActorTypeAndHourDS
  }

  def getMaxCommitPerHour(data: Dataset[GitArchive]): Dataset[(String, BigInt)] = {
    val commitPerHour = getCommitPerHour(data)
    val maxCommitPerHour = commitPerHour.reduce((x, y) => {
      if (x._2 > y._2) {
        x
      } else y
    })
    val result = commitPerHour.filter("count = " + maxCommitPerHour._2)
    result
  }

  def getMinCommitPerHour(data: Dataset[GitArchive]): Dataset[(String, BigInt)] = {
    val commitPerHour = getCommitPerHour(data)
    val minCommitPerHour = commitPerHour.reduce((x, y) => {
      if (x._2 < y._2) {
        x
      } else y
    })
    val result = commitPerHour.filter("count = " + minCommitPerHour._2)
    result
  }
}
