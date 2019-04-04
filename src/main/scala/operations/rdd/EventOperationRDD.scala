package operations.rdd

import classes.{Actor, GitArchive, Repo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, min}

class EventOperationRDD {

  def getEventPerActor(rdd: RDD[GitArchive]): RDD[(Actor, Int)] = {
    val eventPerActor = groupByActor(rdd).map { case (actor: Actor, iterable: Iterable[GitArchive]) => (actor, iterable.size) }
    eventPerActor
  }

  def getEventPerHour(rdd: RDD[GitArchive]): RDD[(String, Int)] = {
    val eventPerHour = groupByHour(rdd).map { case (created_at: String, iterable: Iterable[GitArchive]) => (created_at, iterable.size) }
    eventPerHour
  }

  def getEventPerRepo(rdd: RDD[GitArchive]): RDD[(Repo, Int)] = {
    val eventPerRepo = groupByRepo(rdd).map { case (repo: Repo, iterable: Iterable[GitArchive]) => (repo, iterable.size) }
    eventPerRepo
  }

  def getEventPerActorAndType(rdd: RDD[GitArchive]): RDD[(Actor, String, Int)] = {
    val eventPerActorAndType = groupByActorAndType(rdd).map { case ((actor: Actor, tipo: String), iterable: Iterable[GitArchive]) => (actor, tipo, iterable.size) }
    eventPerActorAndType
  }

  def getEventPerActorTypeAndRepo(rdd: RDD[GitArchive]): RDD[(Actor, String, Repo, Int)] = {
    val eventPerActorTypeAndRepo = groupByActorTypeAndRepo(rdd).map { case ((actor: Actor, tipo: String, repo: Repo), iterable: Iterable[GitArchive]) => (actor, tipo, repo, iterable.size) }
    eventPerActorTypeAndRepo
  }

  def getEventPerActorTypeRepoAndHour(rdd: RDD[GitArchive]): RDD[(Actor, String, Repo, String, Int)] = {
    val eventPerActorTypeRepoAndHour = groupByActorTypeRepoAndHour(rdd).map { case ((actor: Actor, tipo: String, repo: Repo, created_at: String), iterable: Iterable[GitArchive]) => (actor, tipo, repo, created_at, iterable.size) }
    eventPerActorTypeRepoAndHour
  }

  def getMaxEventPerActor(rdd: RDD[GitArchive]): RDD[(Actor, Int)] = {
    val eventPerActor = getEventPerActor(rdd)
    val max = eventPerActor.reduce((x, y) => {
      if (x._2 > y._2) x else y
    })
    val maxEventPerActor = eventPerActor.filter(x => x._2 == max._2)
    maxEventPerActor
  }

  def getMinEventPerActor(rdd: RDD[GitArchive]): RDD[(Actor, Int)] = {
    val eventPerActor = getEventPerActor(rdd)
    val min = eventPerActor.reduce((x, y) => {
      if (x._2 < y._2) x else y
    })
    val minEventPerActor = eventPerActor.filter(x => x._2 == min._2)
    minEventPerActor
  }

  def getMaxEventPerHour(rdd: RDD[GitArchive]): RDD[(String, Int)] = {
    val eventPerHour = getEventPerHour(rdd)
    val max = eventPerHour.reduce((x, y) => {
      if (x._2 > y._2) x else y
    })
    val maxEventPerHour = eventPerHour.filter(x => x._2 == max._2)
    maxEventPerHour
  }

  def getMinEventPerHour(rdd: RDD[GitArchive]): RDD[(String, Int)] = {
    val eventPerHour = getEventPerHour(rdd)
    val min = eventPerHour.reduce((x, y) => {
      if (x._2 < y._2) x else y
    })
    val minEventPerHour = eventPerHour.filter(x => x._2 == min._2)
    minEventPerHour
  }

  def getMinEventPerRepo(rdd: RDD[GitArchive]): RDD[(Repo, Int)] = {
    val eventPerRepo = getEventPerRepo(rdd)
    val min = eventPerRepo.reduce((x, y) => {
      if (x._2 < y._2) x else y
    })
    val minEventPerRepo = eventPerRepo.filter(x => x._2 == min._2)
    minEventPerRepo
  }

  def getMaxEventPerRepo(rdd: RDD[GitArchive]): RDD[(Repo, Int)] = {
    val eventPerRepo = getEventPerRepo(rdd)
    val max = eventPerRepo.reduce((x, y) => {
      if (x._2 > y._2) x else y
    })
    val maxEventPerRepo = eventPerRepo.filter(x => x._2 == max._2)
    maxEventPerRepo
  }

  def getMinEventPerActorAndHour(rdd: RDD[GitArchive]): RDD[(Actor, String, Int)] = {
    val eventPerActorAndHour = groupByActorAndHour(rdd).map { case ((actor: Actor, created_at: String), iterable: Iterable[GitArchive]) => (actor, created_at, iterable.size) }
    val min = eventPerActorAndHour.reduce((x, y) => {
      if (x._3 < y._3) x else y
    })
    val minEventPerActorAndHour = eventPerActorAndHour.filter(x => x._3 == min._3)
    minEventPerActorAndHour
  }

  def getMaxEventPerActorAndHour(rdd: RDD[GitArchive]): RDD[(Actor, String, Int)] = {
    val eventPerActorAndHour = groupByActorAndHour(rdd).map { case ((actor: Actor, created_at: String), iterable: Iterable[GitArchive]) => (actor, created_at, iterable.size) }
    val max = eventPerActorAndHour.reduce((x, y) => {
      if (x._3 > y._3) x else y
    })
    val maxEventPerActorAndHour = eventPerActorAndHour.filter(x => x._3 == max._3)
    maxEventPerActorAndHour
  }

  def getMinEventPerRepoAndHour(rdd: RDD[GitArchive]): RDD[(Repo, String, Int)] = {
    val eventPerRepoAndHour = groupByRepoAndHour(rdd).map { case ((repo: Repo, created_at: String), iterable: Iterable[GitArchive]) => (repo, created_at, iterable.size) }
    val min = eventPerRepoAndHour.reduce((x, y) => {
      if (x._3 < y._3) x else y
    })
    val minEventPerActorAndHour = eventPerRepoAndHour.filter(x => x._3 == min._3)
    minEventPerActorAndHour
  }

  def getMaxEventPerRepoAndHour(rdd: RDD[GitArchive]): RDD[(Repo, String, Int)] = {
    val eventPerRepoAndHour = groupByRepoAndHour(rdd).map { case ((repo: Repo, created_at: String), iterable: Iterable[GitArchive]) => (repo, created_at, iterable.size) }
    val max = eventPerRepoAndHour.reduce((x, y) => {
      if (x._3 > y._3) x else y
    })
    val maxEventPerActorAndHour = eventPerRepoAndHour.filter(x => x._3 == max._3)
    maxEventPerActorAndHour
  }

  def getMinEventPerRepoHourAndActor(rdd: RDD[GitArchive]): RDD[(Repo, String, Actor, Int)] = {
    val eventPerRepoHourAndActor = groupByRepoHourAndActor(rdd).map { case ((repo: Repo, created_at: String, actor: Actor), iterable: Iterable[GitArchive]) => (repo, created_at, actor, iterable.size) }
    val min = eventPerRepoHourAndActor.reduce((x, y) => {
      if (x._4 < y._4) x else y
    })
    val minEventPerRepoHourAndActor = eventPerRepoHourAndActor.filter(x => x._4 == min._4)
    minEventPerRepoHourAndActor
  }

  def getMaxEventPerRepoHourAndActor(rdd: RDD[GitArchive]): RDD[(Repo, String, Actor, Int)] = {
    val eventPerRepoHourAndActor = groupByRepoHourAndActor(rdd).map { case ((repo: Repo, created_at: String, actor: Actor), iterable: Iterable[GitArchive]) => (repo, created_at, actor, iterable.size) }
    val max = eventPerRepoHourAndActor.reduce((x, y) => {
      if (x._4 < y._4) x else y
    })
    val maxEventPerRepoHourAndActor = eventPerRepoHourAndActor.filter(x => x._4 == max._4)
    maxEventPerRepoHourAndActor
  }

  def groupByActor(rdd: RDD[GitArchive]): RDD[(Actor, Iterable[GitArchive])] = {
    val groupByActor = rdd.groupBy { case gitArchive: GitArchive => gitArchive.actor }
    groupByActor
  }

  def groupByHour(rdd: RDD[GitArchive]): RDD[(String, Iterable[GitArchive])] = {
    val groupByHour = rdd.groupBy { case gitArchive: GitArchive => gitArchive.created_at }
    groupByHour
  }

  def groupByRepo(rdd: RDD[GitArchive]): RDD[(Repo, Iterable[GitArchive])] = {
    val groupByRepo = rdd.groupBy { case gitArchive: GitArchive => gitArchive.repo }
    groupByRepo
  }

  def groupByActorAndType(rdd: RDD[GitArchive]): RDD[((Actor, String), Iterable[GitArchive])] = {
    val groupByActorAndType = rdd.groupBy { case gitArchive: GitArchive => (gitArchive.actor, gitArchive.tipo) }
    groupByActorAndType
  }

  def groupByActorAndHour(rdd: RDD[GitArchive]): RDD[((Actor, String), Iterable[GitArchive])] = {
    val groupByActorAndHour = rdd.groupBy { case gitArchive: GitArchive => (gitArchive.actor, gitArchive.created_at) }
    groupByActorAndHour
  }

  def groupByRepoAndHour(rdd: RDD[GitArchive]): RDD[((Repo, String), Iterable[GitArchive])] = {
    val groupByRepoAndHour = rdd.groupBy { case gitArchive: GitArchive => (gitArchive.repo, gitArchive.created_at) }
    groupByRepoAndHour
  }

  def groupByRepoHourAndActor(rdd: RDD[GitArchive]): RDD[((Repo, String, Actor), Iterable[GitArchive])] = {
    val groupByRepoHourAndActor = rdd.groupBy { case gitArchive: GitArchive => (gitArchive.repo, gitArchive.created_at, gitArchive.actor) }
    groupByRepoHourAndActor
  }

  def groupByActorTypeAndRepo(rdd: RDD[GitArchive]): RDD[((Actor, String, Repo), Iterable[GitArchive])] = {
    val groupByActorTypeAndRepo = rdd.groupBy { case gitArchive: GitArchive => (gitArchive.actor, gitArchive.tipo, gitArchive.repo) }
    groupByActorTypeAndRepo
  }

  def groupByActorTypeRepoAndHour(rdd: RDD[GitArchive]): RDD[((Actor, String, Repo, String), Iterable[GitArchive])] = {
    val groupByActorTypeRepoAndHour = rdd.groupBy { case gitArchive: GitArchive => (gitArchive.actor, gitArchive.tipo, gitArchive.repo, gitArchive.created_at) }
    groupByActorTypeRepoAndHour
  }

}
