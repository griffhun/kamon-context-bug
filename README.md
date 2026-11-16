# Kamon attaches the wrong Context to remote Akka / Pekko (Artery) messages

Under the Kanela agent, Kamon's Artery instrumentation for Akka 2.6 and Pekko delivers remote messages under a **Context that
belongs to another message**:

- a message sent without any Context (cluster gossip, heartbeats, a timer's tick) is dispatched on the receiving node under the
  Context of whatever message used the same pooled envelope before it, typically a request's;
- a message sent under a Context is written with the Context of whichever sender woke the outbound stream, not its own.

Akka's own machinery then keeps such a Context alive. Kamon captures the current Context on every `scheduleOnce`, and fixed-delay
timers (the sharding replicator's gossip, a `ShardedDaemonProcess`'s keep-alive) are chains of `scheduleOnce`. In a production
cluster one request's trace kept growing for days (875k spans, 957 MB), and every background actor's log line carried that
request's MDC.

Reproduced with **Kamon 2.7.7 and 2.8.1** (the latest release), Akka 2.6.20 and Pekko 1.1.3, Java 17.

## Run it

```sh
sbt akka/test                               # minimal reproduction on Akka 2.6.20: FAILS (the bug)
sbt pekko/test                              # the same on Pekko 1.1.3: FAILS
FIX=inbound,outbound sbt akka/test          # with the proposed fix: passes (same for pekko/test)

sbt akka/run                                # a cluster with sharding: RESULT: CONTAMINATED, exit code 1
FIX=inbound,outbound sbt akka/run           # RESULT: OK, exit code 0

KAMON_VERSION=2.8.1 sbt ...                 # Kamon 2.8.1 + Kanela 2.0.0 instead of 2.7.7 + 1.0.18
```

Vanilla Akka / Pekko and public Kamon artifacts only. sbt resolves the Kanela agent and attaches it to the forked JVM.

## What it does

### Minimal reproduction: [`ArteryContextSpec`](akka/src/test/scala/repro/ArteryContextSpec.scala) (Akka; [Pekko](pekko/src/test/scala/repro/ArteryContextSpec.scala))

Two actor systems talk over Artery (TCP, loopback). A message sent under a Span must arrive under that Span's trace; a message
sent without one must arrive without a trace.

1. **One message at a time** (20 traced, 20 untraced, alternating): each message reuses the pooled inbound envelope the previous
   one released. This isolates the inbound defect.
2. **Bursts** (20 bursts of 8, every third message traced): the outbound stream, woken by one sender, writes the whole burst.
   This isolates the outbound defect.

### A realistic scenario: [`Main.scala`](akka/src/main/scala/repro/Main.scala) (Akka)

Two cluster nodes in one JVM (Artery on `127.0.0.1:25520` and `:25521`):

- **Requests**: for 12 s, each under its own root Span, `ask` a sharded entity through either node.
- **Background**: a `ShardedDaemonProcess` (keep-alive every 2 s) plus the cluster's own gossip.
- **Control channel**: an actor on node A, started without any Context, sends a `Ping` to node B every 100 ms. Every 10th
  `Ping` is sent under its own Span. Node B records the trace each `Ping` arrives under.

It then checks three things, each expected to be 0: untraced `Ping`s that arrived under a trace, traced `Ping`s that arrived under
the wrong trace, and spans that joined a request's trace more than 2 s after the request finished.

## Results

Minimal reproduction, messages that arrived under another trace (expected 0). Every run gives the same numbers, give or take one
traced message per run.

| | one at a time: untraced | bursts: untraced | bursts: traced |
|---|---|---|---|
| Akka, Kamon 2.7.7 | 20 of 20 | 100 of 100 | 39 of 60 |
| Akka, Kamon 2.8.1 | 20 of 20 | 100 of 100 | 40 of 60 |
| Pekko, Kamon 2.7.7 | 20 of 20 | 100 of 100 | 40 of 60 |
| Pekko, Kamon 2.8.1 | 20 of 20 | 100 of 100 | 40 of 60 |
| Akka, `FIX=inbound` | 0 | 100 of 100 | 40 of 60 |
| Akka, `FIX=outbound` | 20 of 20 | 91 of 100 | 0 |
| either, either version, `FIX=inbound,outbound` | 0 | 0 | 0 |

In a burst without the fix, every message arrives under the trace of the burst's first message. With only the outbound fix, the
untraced ones still pick up the previous message's trace from the reused envelope.

Cluster scenario:

| | untraced pings under a trace | traced pings, wrong trace | spans joining a finished request's trace |
|---|---|---|---|
| Kamon 2.7.7 | 163 of 346 | 1 of 38 | 182 |
| Kamon 2.8.1 | 160 of 348 | 0 of 38 | 182 |
| either + `FIX=inbound,outbound` | 0 | 0 | 0 |

A typical lingering span, with its ancestry:

```
+5184ms tell(StartEntity)  repro/system/sharding/sharded-daemon-process-background     <- keep-alive tick, under the request's Context
+3157ms tell(HostShard)    repro/system/sharding/Entity
+3148ms tell(UpdateSuccess) repro/system/sharding/EntityCoordinator/singleton/coordinator
+3136ms tell(Write)        repro/system/sharding/replicator
+3072ms tell(Update)       repro/system/sharding/replicator
+3071ms tell(GetShardHome) repro/system/sharding/EntityCoordinator/singleton/coordinator
  ...   the request's first `ask` to an unallocated shard
```

## Root cause

Two defects in the Artery instrumentation, the same in kamon-akka for Akka 2.6
([`RemotingInstrumentation`](https://github.com/kamon-io/Kamon/blob/master/instrumentation/kamon-akka/src/akka-2.6/scala/kamon/instrumentation/akka/instrumentations/akka_26/remote/RemotingInstrumentation.scala),
[`KamonRemoteInstrument`](https://github.com/kamon-io/Kamon/blob/master/instrumentation/kamon-akka/src/akka-2.6/scala/akka/remote/artery/KamonRemoteInstrument.scala))
and in kamon-pekko
([`RemotingInstrumentation`](https://github.com/kamon-io/Kamon/blob/master/instrumentation/kamon-pekko/src/main/scala/kamon/instrumentation/pekko/remote/RemotingInstrumentation.scala),
[`KamonRemoteInstrument`](https://github.com/kamon-io/Kamon/blob/master/instrumentation/kamon-pekko/src/main/scala/kamon/instrumentation/pekko/remote/artery/KamonRemoteInstrument.scala)):

1. **Inbound.** The Context lives in a `HasContext` mixin on `ReusableInboundEnvelope`, which Akka / Pekko **pool and reuse**.
   Kamon only sets it in `KamonRemoteInstrument.remoteReadMetadata`, and that is called only when the incoming message carries
   Kamon's metadata. A message sent without a Context therefore keeps the Context of the envelope's previous message, and
   `ArteryMessageDispatcherAdvice` dispatches it under that Context. Nothing resets it.
2. **Outbound.** `KamonRemoteInstrument.remoteWriteMetadata` writes `Kamon.currentContext()` of the outbound stream's thread:
   the Context of whichever sender woke the `Encoder` stream, not the message's own (captured on the envelope by
   `CaptureCurrentContextOnReusableEnvelope`, then never read). Envelopes that do not go through
   `Association.createOutboundEnvelope` (control messages, handshake) never get a Context of their own.

On Akka, the outbound path used the envelope's Context until
[`0b6e75a2` "Remove MessageSerializer advice on 2.6"](https://github.com/kamon-io/Kamon/commit/0b6e75a2) (January 2021).
kamon-pekko still carries a `SerializeForArteryAdvice` that reads the envelope's Context
([`ArterySerializationAdvice.scala`](https://github.com/kamon-io/Kamon/blob/master/instrumentation/kamon-pekko/src/main/scala/kamon/instrumentation/pekko/remote/internal/ArterySerializationAdvice.scala)),
but nothing registers it.

## Proposed fix

[`ArteryContextFix.scala`](fix/src/main/scala/fix/ArteryContextFix.scala), a Kanela module for Akka and Pekko registered in
[`reference.conf`](fix/src/main/resources/reference.conf), enabled per part by `FIX`:

- `inbound`: `ReusableInboundEnvelope.init` (exit) resets the Context to `Context.Empty`. The `Decoder` calls `init` before the
  `Deserializer` reads the metadata, so a carried Context still lands.
- `outbound`: `ReusableOutboundEnvelope.init` (exit) captures `Kamon.currentContext()`, which covers every send path, and
  `MessageSerializer.serializeForArtery` (enter/exit) makes the envelope's Context current while the header, including
  Kamon's metadata, is written.

Either part alone is not enough (see the results above). The module's `within` lists both a regex and a package prefix:
Kanela 1.x (Kamon 2.7) reads the former, Kanela 2.x (Kamon 2.8) the latter.
