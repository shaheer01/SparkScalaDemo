name := "SparkScalaCourse"

version := "0.1"

scalaVersion := "2.12.13"

//scalaVersion := "2.12"

libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-core" % "2.4.8",
//  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
//  "org.apache.spark" %% "spark-mllib" % "3.4.1",
  "org.apache.spark" %% "spark-mllib" % "2.4.8",
//  "org.apache.spark" %% "spark-streaming" % "3.4.1",
  "org.apache.spark" %% "spark-streaming" % "2.4.8",
//  "org.apache.spark" %% "spark-graphx" % "3.4.1",
  "org.apache.spark" %% "spark-graphx" % "2.4.8",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
//  "org.apache.spark" % "spark-avro_2.12" % "3.4.1"
  "org.apache.spark" % "spark-avro_2.12" % "2.4.8"
)
