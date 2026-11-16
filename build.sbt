// Reproduction: Kamon's Artery instrumentation attaches the wrong Context to remote Akka / Pekko messages.
// Vanilla Akka 2.6.20 / Pekko 1.1.3 + public Kamon artifacts from Maven Central.
//
//   sbt akka/test pekko/test                   # minimal reproduction: fails (the bug)
//   FIX=inbound,outbound sbt akka/test pekko/test  # with the proposed fix: passes
//   sbt akka/run                               # the cluster scenario: exits 1 (contaminated); with FIX: exits 0
//   KAMON_VERSION=2.8.1 sbt ...                # Kamon 2.8.1 + Kanela 2.0.0 instead of 2.7.7 + 1.0.18

ThisBuild / scalaVersion := "2.13.16"

val akkaVersion = "2.6.20"
val pekkoVersion = "1.1.3"
val kamonVersion = sys.env.getOrElse("KAMON_VERSION", "2.7.7")
val kanelaVersion = if (kamonVersion.startsWith("2.7")) "1.0.18" else "2.0.0"
val fixParts = sys.env.get("FIX").map("FIX" -> _).toMap

// The Kanela agent jar, attached to every forked JVM with -javaagent
lazy val Agent = config("agent").hide

lazy val underKanela = Seq(
  ivyConfigurations += Agent,
  libraryDependencies ++= Seq(
    "io.kamon" % "kanela-agent" % kanelaVersion % Agent,
    "ch.qos.logback" % "logback-classic" % "1.2.13",
    "org.scalatest" %% "scalatest" % "3.2.19" % Test
  ),
  fork := true,
  javaOptions += {
    val agentJar = update.value
      .select(configurationFilter(Agent.name))
      .find(_.getName.startsWith("kanela-agent"))
      .getOrElse(sys.error("kanela-agent jar not resolved"))
    s"-javaagent:${agentJar.getAbsolutePath}"
  },
  javaOptions ++= Seq(s"-Drepro.kamon=$kamonVersion", s"-Drepro.kanela=$kanelaVersion"),
  run / envVars ++= fixParts,
  Test / envVars ++= fixParts
)

lazy val root = (project in file("."))
  .aggregate(fix, akka, pekko)
  .settings(name := "kamon-context-bug", publish / skip := true)

// The proposed fix: a Kanela module, registered in fix/src/main/resources/reference.conf, advising nothing unless FIX is set
lazy val fix = project
  .settings(
    libraryDependencies ++= Seq(
      "io.kamon" %% "kamon-bundle" % kamonVersion,
      "io.kamon" % "kanela-agent" % kanelaVersion % Provided // the Kanela API; at runtime it comes from the agent
    )
  )

lazy val akka = project
  .dependsOn(fix)
  .settings(underKanela)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
    )
  )

lazy val pekko = project
  .dependsOn(fix)
  .settings(underKanela)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-remote" % pekkoVersion,
      "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion
    )
  )
