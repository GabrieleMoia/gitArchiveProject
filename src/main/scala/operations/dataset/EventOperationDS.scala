package operations.dataset

import classes.{Actor, GitArchive}
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

class EventOperationDS(sparkSession: SparkSession) {

  import sparkSession.sqlContext.implicits._

  def getEventPerActor(data: Dataset[GitArchive]): Dataset[(BigInt, BigInt)] = {
    val eventPerActorDF = data.groupBy("actor.id").count()
    val eventPerActorDS = eventPerActorDF.as[(BigInt, BigInt)]
    eventPerActorDS
  }

  def getEventPerHour(data: Dataset[GitArchive]): Dataset[(String, BigInt)] = {
    val eventPerHourDF = data.groupBy("created_at").count()
    val eventPerHourDS = eventPerHourDF.as[(String, BigInt)]
    eventPerHourDS
  }

  def getEventPerRepo(data: Dataset[GitArchive]): Dataset[(BigInt, BigInt)] = {
    val eventPerRepoDF = data.groupBy("repo.id").count()
    val eventPerRepoDS = eventPerRepoDF.as[(BigInt, BigInt)]
    eventPerRepoDS
  }

  def getEventPerActorAndType(data: Dataset[GitArchive]): Dataset[(BigInt, String, BigInt)] = {
    val actorAndType = data.groupBy("actor.id", "tipo").count()
    val eventPerActorAndType = actorAndType.as[(BigInt, String, BigInt)]
    eventPerActorAndType
  }

  def getEventPerActorAndHour(data: Dataset[GitArchive]): Dataset[(BigInt, String, BigInt)] = {
    val actorAndHour = data.groupBy("actor.id", "created_at").count()
    val eventPerActorAndHour = actorAndHour.as[(BigInt, String, BigInt)]
    eventPerActorAndHour
  }

  def getEventPerRepoAndHour(data: Dataset[GitArchive]): Dataset[(BigInt, String, BigInt)] = {
    val repoAndHour = data.groupBy("repo.id", "created_at").count()
    val eventPerRepoAndHour = repoAndHour.as[(BigInt, String, BigInt)]
    eventPerRepoAndHour
  }

  def getEventPerActorRepoAndHour(data: Dataset[GitArchive]): Dataset[(BigInt, BigInt, String, BigInt)] = {
    val actorRepoAndHour = data.groupBy("actor.id", "repo.id", "created_at").count()
    val eventPerActorRepoAndHour = actorRepoAndHour.as[(BigInt, BigInt, String, BigInt)]
    eventPerActorRepoAndHour
  }

  def getEventPerActorTypeAndRepo(data: Dataset[GitArchive]): Dataset[(BigInt, String, BigInt, BigInt)] = {
    val actorTypeAndRepo = data.groupBy("actor.id", "tipo", "repo.id").count()
    val eventPerActorTypeAndRepo = actorTypeAndRepo.as[(BigInt, String, BigInt, BigInt)]
    eventPerActorTypeAndRepo
  }

  def getEventPerActorTypeRepoAndHour(data: Dataset[GitArchive]): Dataset[(BigInt, String, BigInt, String, BigInt)] = {
    val actorTypeRepoAndHour = data.groupBy("actor.id", "tipo", "repo.id", "created_at").count()
    val eventPerActorTypeRepoAndHour = actorTypeRepoAndHour.as[(BigInt, String, BigInt, String, BigInt)]
    eventPerActorTypeRepoAndHour
  }

  def getMaxEventPerActor(data: Dataset[GitArchive]): Dataset[(BigInt, BigInt)] = {
    val eventPerActor = getEventPerActor(data)
    val maxEventPerActor = eventPerActor.reduce((x, y) => {
      if (x._2 > y._2) {
        x
      } else y
    })
    val result = eventPerActor.filter("count = " + maxEventPerActor._2)
    result
  }

  def getMinEventPerActor(data: Dataset[GitArchive]): Dataset[(BigInt, BigInt)] = {
    val eventPerActor = getEventPerActor(data)
    val minEventPerActor = eventPerActor.reduce((x, y) => {
      if (x._2 < y._2) {
        x
      } else y
    })
    val result = eventPerActor.filter("count = " + minEventPerActor._2)
    result
  }

  def getMaxEventPerHour(data: Dataset[GitArchive]): Dataset[(String, BigInt)] = {
    val eventPerHour = getEventPerHour(data)
    val maxEventPerHour = eventPerHour.reduce((x, y) => {
      if (x._2 > y._2) {
        x
      } else y
    })
    val result = eventPerHour.filter("count = " + maxEventPerHour._2)
    result
  }

  def getMinEventPerHour(data: Dataset[GitArchive]): Dataset[(String, BigInt)] = {
    val eventPerHour = getEventPerHour(data)
    val minEventPerHour = eventPerHour.reduce((x, y) => {
      if (x._2 < y._2) {
        x
      } else y
    })
    val result = eventPerHour.filter("count = " + minEventPerHour._2)
    result
  }

  def getMaxEventPerRepo(data: Dataset[GitArchive]): Dataset[(BigInt, BigInt)] = {
    val eventPerRepo = getEventPerRepo(data)
    val maxEventPerRepo = eventPerRepo.reduce((x, y) => {
      if (x._2 > y._2) {
        x
      } else y
    })
    val result = eventPerRepo.filter("count = " + maxEventPerRepo._2)
    result
  }

  def getMinEventPerRepo(data: Dataset[GitArchive]): Dataset[(BigInt, BigInt)] = {
    val eventPerRepo = getEventPerRepo(data)
    val minEventPerRepo = eventPerRepo.reduce((x, y) => {
      if (x._2 < y._2) {
        x
      } else y
    })
    val result = eventPerRepo.filter("count = " + minEventPerRepo._2)
    result
  }

  def getMaxEventPerActorAndHour(data: Dataset[GitArchive]): Dataset[(BigInt, String, BigInt)] = {
    val eventPerActorAndHour = getEventPerActorAndHour(data)
    val maxEventPerActorAndHour = eventPerActorAndHour.reduce((x, y) => {
      if (x._3 > y._3) {
        x
      } else y
    })
    val result = eventPerActorAndHour.filter("count = " + maxEventPerActorAndHour._3)
    result
  }

  def getMinEventPerActorAndHour(data: Dataset[GitArchive]): Dataset[(BigInt, String, BigInt)] = {
    val eventPerActorAndHour = getEventPerActorAndHour(data)
    val minEventPerActorAndHour = eventPerActorAndHour.reduce((x, y) => {
      if (x._3 < y._3) {
        x
      } else y
    })
    val result = eventPerActorAndHour.filter("count = " + minEventPerActorAndHour._3)
    result
  }

  def getMaxEventPerRepoAndHour(data: Dataset[GitArchive]): Dataset[(BigInt, String, BigInt)] = {
    val eventPerRepoAndHour = getEventPerRepoAndHour(data)
    val maxEventPerRepoAndHour = eventPerRepoAndHour.reduce((x, y) => {
      if (x._3 > y._3) {
        x
      } else y
    })
    val result = eventPerRepoAndHour.filter("count = " + maxEventPerRepoAndHour._3)
    result
  }

  def getMinEventPerRepoAndHour(data: Dataset[GitArchive]): Dataset[(BigInt, String, BigInt)] = {
    val eventPerRepoAndHour = getEventPerRepoAndHour(data)
    val minEventPerRepoAndHour = eventPerRepoAndHour.reduce((x, y) => {
      if (x._3 < y._3) {
        x
      } else y
    })
    val result = eventPerRepoAndHour.filter("count = " + minEventPerRepoAndHour._3)
    result
  }

  def getMaxEventPerActorRepoAndHour(data: Dataset[GitArchive]): Dataset[(BigInt, BigInt, String, BigInt)] = {
    val eventPerActorRepoAndHour = getEventPerActorRepoAndHour(data)
    val maxEventPerActorRepoAndHour = eventPerActorRepoAndHour.reduce((x, y) => {
      if (x._4 > y._4) {
        x
      } else y
    })
    val result = eventPerActorRepoAndHour.filter("count = " + maxEventPerActorRepoAndHour._4)
    result
  }

  def getMinEventPerActorRepoAndHour(data: Dataset[GitArchive]): Dataset[(BigInt, BigInt, String, BigInt)] = {
    val eventPerActorRepoAndHour = getEventPerActorRepoAndHour(data)
    val minEventPerActorRepoAndHour = eventPerActorRepoAndHour.reduce((x, y) => {
      if (x._4 < y._4) {
        x
      } else y
    })
    val result = eventPerActorRepoAndHour.filter("count = " + minEventPerActorRepoAndHour._4)
    result
  }
}
