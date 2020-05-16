name := "aggregateByKey-spark-api-v2"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark"%%"spark-core"%"2.4.3"
)

mainClass in (Compile, run) := Some("com.api.AppDriver")
