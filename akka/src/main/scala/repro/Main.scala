package repro

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorRefResolver, ActorSystem, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey, ShardedDaemonProcess}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import kamon.Kamon
import kamon.module.SpanReporter
import kamon.tag.Lookups
import kamon.trace.Span

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/**
 * Two cluster nodes in one JVM (Artery on 127.0.0.1:25520 and :25521), under the Kanela agent.
 *
 *  - Requests: for 12s, each under its own root Span, `ask` a sharded entity through either node.
 *  - Background: a ShardedDaemonProcess (keep-alive every 2s) and the cluster's own gossip.
 *  - Control channel: an actor on node A, started without any Context, sends a Ping to node B every 100ms; every 10th Ping is
 *    sent under its own Span. Node B records the trace each Ping arrives under.
 *
 * Expected: an untraced Ping arrives without a trace, a traced Ping under its own trace, and no span joins a request's trace
 * after the request finished. Exit code 0 if so, 1 otherwise.
 */
object Main {

  final case class Get(replyTo: ActorRef[String])
  val EntityKey: EntityTypeKey[Get] = EntityTypeKey[Get]("Entity")

  final case class Ping(n: Int)

  final case class Received(at: Instant, n: Int, traceId: String)

  private val finished = new ConcurrentLinkedQueue[Span.Finished]()
  private val received = new ConcurrentLinkedQueue[Received]()
  private val tracedPings = new ConcurrentHashMap[Int, String]()
  private val pingsSentUnderSomeSpan = new AtomicInteger()

  private def currentTraceId(): String = {
    val span = Kamon.currentSpan()
    if (span.isEmpty) "-" else span.trace.id.string
  }

  def entity: Behavior[Get] = Behaviors.receiveMessage { case Get(replyTo) =>
    replyTo ! "ok"
    Behaviors.same
  }

  def background(i: Int): Behavior[String] = Behaviors.ignore

  def pinger(target: ActorRef[Ping]): Behavior[String] = Behaviors.withTimers { timers =>
    timers.startTimerWithFixedDelay("tick", 100.millis)
    var n = 0
    Behaviors.receiveMessage { _ =>
      n += 1
      if (!Kamon.currentSpan().isEmpty) pingsSentUnderSomeSpan.incrementAndGet()
      if (n % 10 == 0) {
        val span = Kamon.spanBuilder("traced-ping").start()
        tracedPings.put(n, span.trace.id.string)
        Kamon.runWithSpan(span)(target ! Ping(n))
      } else target ! Ping(n)
      Behaviors.same
    }
  }

  val ponger: Behavior[Ping] = Behaviors.receiveMessage { case Ping(n) =>
    received.add(Received(Instant.now(), n, currentTraceId()))
    Behaviors.same
  }

  def startNode(port: Int): ActorSystem[Nothing] = {
    val config: Config =
      ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port").withFallback(ConfigFactory.load())
    val system = ActorSystem[Nothing](Behaviors.empty, "repro", config)
    ClusterSharding(system).init(Entity(EntityKey)(_ => entity))
    ShardedDaemonProcess(system).init[String]("background", 4, background)
    system
  }

  def main(args: Array[String]): Unit = {
    println(s"=== Akka ${akka.Version.current}, Kamon ${sys.props.getOrElse("repro.kamon", "?")}, Kanela ${sys.props.getOrElse("repro.kanela", "?")}; FIX=${sys.env.getOrElse("FIX", "none")}")
    Kamon.init()
    Kamon.addReporter(
      "collector",
      new SpanReporter {
        override def reportSpans(spans: Seq[Span.Finished]): Unit = spans.foreach(finished.add)
        override def stop(): Unit = ()
        override def reconfigure(newConfig: Config): Unit = ()
      }
    ): Unit

    val t0 = Instant.now()
    val a = startNode(25520)
    Thread.sleep(1000)
    val b = startNode(25521)

    // control channel, started from main: no Context
    val pongerOnB = b.systemActorOf(ponger, "ponger")
    val pongerSeenFromA = ActorRefResolver(a).resolveActorRef[Ping](ActorRefResolver(b).toSerializationFormat(pongerOnB))
    a.systemActorOf(pinger(pongerSeenFromA), "pinger"): Unit

    implicit val timeout: Timeout = 5.seconds
    val requestThreads = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
    val requestsUntil = t0.plusSeconds(12)
    var i = 0
    while (Instant.now().isBefore(requestsUntil)) {
      val n = i
      Future {
        val span = Kamon.spanBuilder("request").start()
        Kamon.runWithSpan(span, finishSpan = false) {
          val node = if (n % 2 == 0) a else b
          ClusterSharding(node)
            .entityRefFor(EntityKey, s"e${n % 40}")
            .ask[String](Get(_))
            .onComplete(_ => span.finish())(node.executionContext)
        }
      }(requestThreads)
      i += 1
      Thread.sleep(30)
    }
    println(s"=== $i requests in 12s; observing 30s more")
    Thread.sleep(30000)
    Thread.sleep(2500) // let the span reporter flush

    val ok = report(t0)

    b.terminate()
    a.terminate()
    Await.ready(a.whenTerminated, 20.seconds)
    Kamon.stop(): Unit
    sys.exit(if (ok) 0 else 1)
  }

  /** Prints the findings; true when no Context was attached to the wrong message. */
  def report(t0: Instant): Boolean = {
    def rel(i: Instant) = s"+${Duration.between(t0, i).toMillis}ms"
    val spans = finished.asScala.toList
    val byId = spans.map(s => s.id.string -> s).toMap
    def path(s: Span.Finished) = Option(s.tags.get(Lookups.plain("akka.actor.path"))).getOrElse("-")

    val traced = tracedPings.asScala.toMap
    val pongs = received.asScala.toList
    val untraced = pongs.filterNot(p => traced.contains(p.n))
    val untracedUnderATrace = untraced.filter(_.traceId != "-")
    val tracedArrived = pongs.filter(p => traced.contains(p.n))
    val tracedWrong = tracedArrived.filter(p => traced(p.n) != p.traceId)

    val requestEnd = spans.filter(_.operationName == "request").map(s => s.trace.id.string -> s.to).toMap
    val lingering = spans.filter(s =>
      s.operationName != "request" && requestEnd.get(s.trace.id.string).exists(end => s.from.isAfter(end.plusSeconds(2)))
    )

    println(s"=== control channel: ${pongs.size} pings received (${pingsSentUnderSomeSpan.get} of the untraced ones were sent under a span)")
    println(s"    untraced pings that arrived under a trace: ${untracedUnderATrace.size} of ${untraced.size}   (expected 0)")
    untracedUnderATrace.groupBy(_.traceId).toList.sortBy(_._2.map(_.at).min).take(5).foreach { case (tid, ps) =>
      val kind = if (requestEnd.contains(tid)) "a request's" else if (traced.values.exists(_ == tid)) "a traced ping's" else "another"
      println(s"      trace $tid ($kind): ${ps.size} pings, ${rel(ps.map(_.at).min)} .. ${rel(ps.map(_.at).max)}")
    }
    println(s"    traced pings that arrived under the wrong trace: ${tracedWrong.size} of ${tracedArrived.size}   (expected 0)")
    println(s"=== spans that joined a request's trace more than 2s after the request finished: ${lingering.size}   (expected 0)")
    lingering
      .groupBy(s => (s.operationName, path(s).replaceAll("/[0-9]+$", "/N")))
      .toList
      .sortBy(-_._2.size)
      .take(8)
      .foreach { case ((op, p), v) => println(s"      ${v.size} $op $p") }
    lingering.sortBy(_.from).headOption.foreach { first =>
      println("    ancestry of the first one:")
      var cur: Option[Span.Finished] = Some(first)
      var depth = 0
      while (cur.isDefined && depth < 12) {
        val s = cur.get
        println(s"      ${rel(s.from)} ${s.operationName} ${path(s)}")
        cur = byId.get(s.parentId.string)
        depth += 1
      }
    }

    val ok = untracedUnderATrace.isEmpty && tracedWrong.isEmpty && lingering.isEmpty
    println(if (ok) "=== RESULT: OK, no Context crossed to another message" else "=== RESULT: CONTAMINATED, Contexts crossed to other messages")
    ok
  }
}
