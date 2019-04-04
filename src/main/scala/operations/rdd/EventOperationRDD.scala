package operations.rdd

import classes.{Actor, GitArchive, Repo}
import org.apache.spark.rdd.RDD

class EventOperationRDD {

  def getEventPerActor(rdd: RDD[GitArchive]): RDD[(Actor, Int)] = {
    val eventPerActor = groupByActor(rdd).map { case (actor: Actor, iterable: Iterable[GitArchive]) => (actor, iterable.size) }
    eventPerActor
  }

  def getEventPerHour(rdd: RDD[GitArchive]): RDD[(String, Int)] = {
    val eventPerHour = groupByHour(rdd).map { case (created_at: String, iterable: Iterable[GitArchive]) => (created_at, iterable.size) }
    eventPerHour
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

  def groupByActor(rdd: RDD[GitArchive]): RDD[(Actor, Iterable[GitArchive])] = {
    val groupByActor = rdd.groupBy { case gitArchive: GitArchive => gitArchive.actor }
    groupByActor
  }

  def groupByHour(rdd: RDD[GitArchive]): RDD[(String, Iterable[GitArchive])] = {
    val groupByHour = rdd.groupBy { case gitArchive: GitArchive => gitArchive.created_at }
    groupByHour
  }

  def groupByActorAndType(rdd: RDD[GitArchive]): RDD[((Actor, String), Iterable[GitArchive])] = {
    val groupByActorAndType = rdd.groupBy { case gitArchive: GitArchive => (gitArchive.actor, gitArchive.tipo) }
    groupByActorAndType
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
