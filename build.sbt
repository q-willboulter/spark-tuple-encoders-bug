ThisBuild / organization := "org.willbo"
ThisBuild / version := "0.3.0"
ThisBuild / scalaVersion := "2.12.18"

// ********
// Versions
// ********
val SparkVersion = "3.3.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-sql" % SparkVersion,
  "org.apache.spark" %% "spark-catalyst" % SparkVersion
)
