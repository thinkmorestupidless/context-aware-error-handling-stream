name := "context-aware-error-handling-stream"

version := "0.1-SNAPSHOT"

scalaVersion := "2.13.4"


lazy val akkaVersion = "2.6.18"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream"    % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed"         % akkaVersion,
  "ch.qos.logback"    % "logback-classic" % "1.2.3",
)

javacOptions += "-Xlint:deprecation"

scalacOptions += "-deprecation"

