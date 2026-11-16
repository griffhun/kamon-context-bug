// Reproduction: Kamon's Artery instrumentation attaches the wrong Context to remote Akka messages.
// Vanilla Akka 2.6.20 + public Kamon artifacts from Maven Central.
//
//   sbt run                                    # Kamon 2.7.7 + Kanela 1.0.18, no fix  -> exits 1 (contaminated)
//   FIX=inbound,outbound sbt run               # with the proposed fix               -> exits 0
//   KAMON_VERSION=2.8.1 sbt run                # Kamon 2.8.1 + Kanela 2.0.0

ThisBuild / scalaVersion := "2.13.16"

val akkaVersion = "2.6.20"
val kamonVersion = sys.env.getOrElse("KAMON_VERSION", "2.7.7")
val kanelaVersion = if (kamonVersion.startsWith("2.7")) "1.0.18" else "2.0.0"

// The Kanela agent jar, attached to the forked JVM with -javaagent
lazy val Agent = config("agent").hide

lazy val root = (project in file("."))
  .configs(Agent)
  .settings(
    name := "kamon-context-bug",
    ivyConfigurations += Agent,
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "io.kamon" %% "kamon-bundle" % kamonVersion,
      "ch.qos.logback" % "logback-classic" % "1.2.13",
      // compile-time only: the proposed fix (src/main/scala/fix) uses the Kanela API
      "io.kamon" % "kanela-agent" % kanelaVersion % Provided,
      "io.kamon" % "kanela-agent" % kanelaVersion % Agent
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
    run / envVars ++= sys.env.get("FIX").map("FIX" -> _).toMap
  )
