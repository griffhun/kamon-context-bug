# Kamon attaches the wrong Context to remote Akka (Artery) messages

Under the Kanela agent, Kamon's Artery instrumentation for Akka 2.6 delivers remote messages under a **Context that belongs to
another message**. A message sent without any Context (cluster gossip, heartbeats, a timer's tick) is dispatched on the
receiving node under the Context of whatever message used the same pooled envelope before it, typically a request's.

Akka's own machinery then keeps that Context alive. Kamon captures the current Context on every `scheduleOnce`, and
fixed-delay timers (the sharding replicator's gossip, a `ShardedDaemonProcess`'s keep-alive) are chains of `scheduleOnce`.
In a production cluster one request's trace kept growing for days (875k spans, 957 MB), and every background actor's log line
carried that request's MDC.

Reproduced with **Kamon 2.7.7 and 2.8.1** (the latest release), Akka 2.6.20, Java 17. kamon-pekko has the same code, so Pekko
is very likely affected the same way (not run here).

## Run it

```sh
sbt run                                   # Kamon 2.7.7 + Kanela 1.0.18          -> RESULT: CONTAMINATED, exit code 1
KAMON_VERSION=2.8.1 sbt run               # Kamon 2.8.1 + Kanela 2.0.0           -> RESULT: CONTAMINATED, exit code 1
FIX=inbound,outbound sbt run              # with the proposed fix (either version) -> RESULT: OK, exit code 0
```

Vanilla Akka 2.6.20 and public Kamon artifacts only. The Kanela agent is resolved by sbt and attached to the forked JVM.

## What it does

[`Main.scala`](src/main/scala/repro/Main.scala) starts two cluster nodes in one JVM (Artery on `127.0.0.1:25520` and `:25521`):

- **Requests**: for 12 s, each under its own root Span, `ask` a sharded entity through either node.
- **Background**: a `ShardedDaemonProcess` (keep-alive every 2 s) plus the cluster's own gossip.
- **Control channel**: an actor on node A, started without any Context, sends a `Ping` to node B every 100 ms. Every 10th
  `Ping` is sent under its own Span. Node B records the trace each `Ping` arrives under.

It then checks three things, each expected to be 0:

1. untraced `Ping`s that arrived under a trace;
2. traced `Ping`s that arrived under the wrong trace;
3. spans that joined a request's trace more than 2 s after the request finished.

## Results

| | untraced pings under a trace | traced pings, wrong trace | spans joining a finished request's trace |
|---|---|---|---|
| Kamon 2.7.7 | 157 of 346 (one request's trace on pings from +3 s to +43 s) | 0 of 38 | 182 |
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

Two defects in the Akka 2.6 Artery instrumentation
([`RemotingInstrumentation`](https://github.com/kamon-io/Kamon/blob/master/instrumentation/kamon-akka/src/akka-2.6/scala/kamon/instrumentation/akka/instrumentations/akka_26/remote/RemotingInstrumentation.scala),
[`KamonRemoteInstrument`](https://github.com/kamon-io/Kamon/blob/master/instrumentation/kamon-akka/src/akka-2.6/scala/akka/remote/artery/KamonRemoteInstrument.scala)):

1. **Inbound (the main one).** The Context lives in a `HasContext` mixin on `akka.remote.artery.ReusableInboundEnvelope`,
   which Akka **pools and reuses**. Kamon only sets it in `KamonRemoteInstrument.remoteReadMetadata`, and Akka calls that
   only when the incoming message carries Kamon's metadata. A message sent without a Context therefore keeps the Context of
   the envelope's previous message, and `ArteryMessageDispatcherAdvice` dispatches it under that Context. Nothing resets it.
2. **Outbound.** `KamonRemoteInstrument.remoteWriteMetadata` writes `Kamon.currentContext()` of the outbound stream's
   thread: the Context of whichever sender woke the `Encoder` stream, not the message's own (captured on the envelope by
   `CaptureCurrentContextOnReusableEnvelope`, then never read). Envelopes that do not go through
   `Association.createOutboundEnvelope` (control messages, handshake) never get a Context of their own. With only the
   inbound fix, a traced message occasionally arrives without its Context and an untraced one occasionally with someone
   else's.

The outbound path used the envelope's Context until
[`0b6e75a2` "Remove MessageSerializer advice on 2.6"](https://github.com/kamon-io/Kamon/commit/0b6e75a2) (January 2021).

## Proposed fix

[`ArteryContextFix.scala`](src/main/scala/fix/ArteryContextFix.scala), a Kanela module registered in
[`application.conf`](src/main/resources/application.conf), enabled per part by `FIX`:

- `inbound`: `ReusableInboundEnvelope.init` (exit) resets the Context to `Context.Empty`. The `Decoder` calls `init` before the
  `Deserializer` reads the metadata, so a carried Context still lands.
- `outbound`: `ReusableOutboundEnvelope.init` (exit) captures `Kamon.currentContext()`, which covers every send path, and
  `MessageSerializer.serializeForArtery` (enter/exit) makes the envelope's Context current while the header, including
  Kamon's metadata, is written.

Either part alone is not enough: with `FIX=inbound` the long-lived leak is gone but single messages still get the wrong
Context now and then; with `FIX=outbound` the leak stays.
