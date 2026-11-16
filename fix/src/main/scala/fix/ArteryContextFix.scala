package fix

import kamon.Kamon
import kamon.context.{Context, Storage}
import kamon.instrumentation.context.HasContext
import kanela.agent.api.instrumentation.InstrumentationBuilder
import kanela.agent.libs.net.bytebuddy.asm.Advice

import scala.annotation.static

/**
 * Proposed fix for Kamon's Artery context propagation (Akka 2.6 and Pekko), enabled per part by the FIX env var
 * ("inbound", "outbound"). Registered as Kanela modules in reference.conf.
 *
 * inbound:  Kamon mixes a Context into the POOLED ReusableInboundEnvelope and only sets it in
 *           KamonRemoteInstrument.remoteReadMetadata, which is called only when the message carries a Context. A message
 *           sent without one is then dispatched under the Context of the previous message that used the same pooled envelope.
 *           Fix: reset it in ReusableInboundEnvelope.init (the Decoder calls init before the Deserializer reads the metadata).
 *
 * outbound: KamonRemoteInstrument.remoteWriteMetadata writes Kamon.currentContext() of the outbound stream's thread, i.e. the
 *           Context of whichever sender woke the Encoder stream, not the message's own. Fix: capture the sender's Context in
 *           ReusableOutboundEnvelope.init (every send path calls it, not only Association.createOutboundEnvelope) and make it
 *           current while MessageSerializer.serializeForArtery writes the envelope, so remoteWriteMetadata writes the right one.
 */
abstract class ArteryContextFix(remotePackage: String) extends InstrumentationBuilder {
  private val parts = Option(System.getenv("FIX")).getOrElse("").split(',').map(_.trim).toSet

  if (parts("inbound"))
    onType(s"$remotePackage.artery.ReusableInboundEnvelope")
      .advise(method("init"), classOf[ResetContextOnInboundInit])

  if (parts("outbound")) {
    onType(s"$remotePackage.artery.ReusableOutboundEnvelope")
      .advise(method("init"), classOf[CaptureContextOnOutboundInit])
    onType(s"$remotePackage.MessageSerializer$$")
      .advise(method("serializeForArtery"), classOf[SerializeUnderEnvelopeContext])
  }
}

final class AkkaArteryContextFix extends ArteryContextFix("akka.remote")
final class PekkoArteryContextFix extends ArteryContextFix("org.apache.pekko.remote")

class ResetContextOnInboundInit
object ResetContextOnInboundInit {
  @Advice.OnMethodExit
  @static def exit(@Advice.This envelope: Any): Unit = envelope match {
    case hc: HasContext => hc.setContext(Context.Empty)
    case _              => ()
  }
}

class CaptureContextOnOutboundInit
object CaptureContextOnOutboundInit {
  @Advice.OnMethodExit
  @static def exit(@Advice.This envelope: Any): Unit = envelope match {
    case hc: HasContext => hc.setContext(Kamon.currentContext())
    case _              => ()
  }
}

class SerializeUnderEnvelopeContext
object SerializeUnderEnvelopeContext {
  @Advice.OnMethodEnter
  @static def enter(@Advice.Argument(1) outboundEnvelope: Any): Storage.Scope = {
    val context = outboundEnvelope match {
      case hc: HasContext if hc.context != null => hc.context
      case _                                    => Context.Empty
    }
    Kamon.storeContext(context)
  }

  @Advice.OnMethodExit(onThrowable = classOf[Throwable])
  @static def exit(@Advice.Enter scope: Storage.Scope): Unit = scope.close()
}
