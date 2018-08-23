name := """akka-csv-diff"""

organization := "ch.taggiasco"

version := "0.0.1"

scalaVersion := "2.12.6"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaVersion     = "2.5.14"
  val alpakkaVersion  = "0.20"

  Seq(
    "com.typesafe.akka"  %% "akka-stream"             % akkaVersion,
    "com.lightbend.akka" %% "akka-stream-alpakka-csv" % alpakkaVersion,
    "org.scalatest"      %% "scalatest"               % "3.0.1"     % "test",
    "com.typesafe.akka"  %% "akka-stream-testkit"     % akkaVersion
  )
}
