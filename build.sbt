name := "SparkScalaCourse"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "org.apache.spark" %% "spark-mllib" % "3.4.1",
  "org.apache.spark" %% "spark-streaming" % "3.4.1",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4"
)
