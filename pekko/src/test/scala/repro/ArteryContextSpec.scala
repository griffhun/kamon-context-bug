package repro

import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, ExtendedActorSystem, Props, RootActorPath}
import kamon.Kamon
import kamon.context.Context
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * Minimal reproduction, under the Kanela agent: two actor systems talk over Artery (TCP, loopback). A message sent under a Span
 * must arrive under that Span's trace; a message sent without one must arrive without a trace.
 *
 *  - one message at a time: each message reuses the pooled inbound envelope the previous one released (the inbound defect);
 *  - bursts: the outbound stream, woken by one sender, writes the messages of the whole burst (the outbound defect).
 *
 * Fails on Kamon 2.7.7 and 2.8.1; passes with FIX=inbound,outbound.
 */
class ArteryContextSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with Eventually {
  import ArteryContextSpec._

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  Kamon.init()

  private val received = new ConcurrentLinkedQueue[Received]()
  private val senderSystem = ActorSystem("sender")
  private val receiverSystem = ActorSystem("receiver")

  private val recorder: ActorRef = {
    receiverSystem.actorOf(Props(new Recorder(received)), "recorder")
    val address = receiverSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    Await.result(senderSystem.actorSelection(RootActorPath(address) / "user" / "recorder").resolveOne(10.seconds), 15.seconds)
  }

  override def afterAll(): Unit = {
    senderSystem.terminate()
    receiverSystem.terminate()
    Await.ready(senderSystem.whenTerminated, 15.seconds)
    Await.ready(receiverSystem.whenTerminated, 15.seconds)
    ()
  }

  /** Sends `message` to the remote recorder, under a new Span if `traced`; returns the trace it should arrive under. */
  private def send(message: String, traced: Boolean): Expected = {
    Kamon.currentContext() shouldBe Context.Empty
    if (traced) {
      val span = Kamon.spanBuilder("traced-message").start()
      Kamon.runWithSpan(span)(recorder ! message)
      Expected(message, Some(span.trace.id.string))
    } else {
      recorder ! message
      Expected(message, None)
    }
  }

  private def awaitAll(expected: Seq[Expected]): Assertion =
    eventually(received.asScala.map(_.message).toSet should contain allElementsOf expected.map(_.message))

  private def verify(expected: Seq[Expected]): Assertion = {
    val arrived = received.asScala.map(r => r.message -> r.traceId).toMap
    val wrong = expected.filter(e => arrived(e.message) != e.traceId)
    def describe(es: Seq[Expected], kind: String) =
      s"${es.size} of ${expected.count(_.traceId.isDefined == (kind == "traced"))} $kind"
    val (wrongTraced, wrongUntraced) = wrong.partition(_.traceId.isDefined)
    withClue(
      s"${describe(wrongUntraced, "untraced")} and ${describe(wrongTraced, "traced")} messages arrived under another trace, e.g. " +
        wrong.take(4).map(e => s"${e.message}: sent under ${e.traceId.getOrElse("none")}, arrived under ${arrived(e.message).getOrElse("none")}").mkString("; ") +
        "\n"
    )(wrong shouldBe empty)
  }

  "Remote Artery messages" should {
    "arrive without a trace when sent without one, after a traced message used the same pooled envelope" in {
      val expected = (1 to 20).flatMap { i =>
        val traced = send(s"sequential-traced-$i", traced = true)
        val _ = awaitAll(Seq(traced))
        val untraced = send(s"sequential-untraced-$i", traced = false)
        val _ = awaitAll(Seq(untraced))
        Seq(traced, untraced)
      }
      verify(expected)
    }

    "arrive under their own trace when sent in a burst" in {
      val expected = (1 to 20).flatMap { i =>
        val burst = (1 to 8).map(j => send(s"burst-$i-$j", traced = j % 3 == 1))
        val _ = awaitAll(burst)
        burst
      }
      verify(expected)
    }
  }
}

object ArteryContextSpec {
  final case class Received(message: String, traceId: Option[String])
  final case class Expected(message: String, traceId: Option[String])

  private def currentTraceId: Option[String] = {
    val span = Kamon.currentSpan()
    if (span.isEmpty) None else Some(span.trace.id.string)
  }

  final class Recorder(received: ConcurrentLinkedQueue[Received]) extends Actor {
    override def receive: Receive = { case message: String =>
      received.add(Received(message, currentTraceId))
      ()
    }
  }
}
