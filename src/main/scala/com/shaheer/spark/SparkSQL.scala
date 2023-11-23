package com.shaheer.spark

import com.fasterxml.jackson.annotation.JsonProperty
import com.shaheer.spark.Action.Action
import com.shaheer.spark.EventType.EventType
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}

import java.io.{FileNotFoundException, IOException}

object SparkSQL {

  case class ECEvent(@JsonProperty modelName: String, @JsonProperty docKey: String, @JsonProperty isUpdate: Boolean, @JsonProperty isUpsert: Boolean, @JsonProperty etlTime: Long, @JsonProperty etlTimeHuman: String, @JsonProperty accountId: String, @JsonProperty timeStamp: Long, @JsonProperty doc: DocIntermediate)

  case class DocIntermediate(campaignId: Long, engagementId: Long, skillId: Long, rule: String, action: Action, actionCount: Int, timeInState: Long, eventType: EventType, reportTime: String)


  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ECIntermediateSchema = StructType(Seq(
      StructField("modelName", StringType),
      StructField("docKey", StringType),
      StructField("isUpdate", BooleanType),
      StructField("isUpsert", BooleanType),
      StructField("etlTime", LongType),
      StructField("etlTimeHuman", StringType),
      StructField("accountId", StringType),
      StructField("timeStamp", LongType),
      StructField("doc", StructType(Seq(
        StructField("campaignId", LongType),
        StructField("engagementId", LongType),
        StructField("skillId", LongType),
        StructField("rule", StringType),
        StructField("action", StringType),
        StructField("actionCount", IntegerType),
        StructField("timeInState", LongType),
        StructField("eventType", StringType),
        StructField("reportTime", StringType)
      )))
    ))
    
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
    
//    val lines = spark.sparkContext.textFile("data/fakefriends-noheader.csv")
//
//
//      val sparkSession: SparkSession = InjectorProvider.get().getInstance(classOf[SparkSession])
//      import sparkSession.implicits._

      try {
        val eventDS = spark.sqlContext.read.schema(ECIntermediateSchema).json("/Users/smannan/Downloads/LPrepos/engagement-controller-reporting/src/main/resources/output/intermediateOutput.json")
        eventDS.createOrReplaceTempView("events")
        eventDS.printSchema()
        val result = spark.sql(
          """
            |SELECT
            |  doc.campaignId AS campaignId,
            |  doc.engagementId AS engagementId,
            |  doc.skillId AS skillId,
            |  doc.action AS Action,
            |  SUM(doc.actionCount) AS TotalActionCount,
            |  SUM(doc.timeInState) AS TotalTimeInState
            |FROM
            |  events
            |GROUP BY
            |  doc.campaignId,
            |  doc.engagementId,
            |  doc.skillId,
            |  doc.action
            |""".stripMargin
        )

        // Join "events" DataFrame with "result" DataFrame based on campaignId, skillId, and engagementId
        val joinedDF = eventDS.join(result,
          eventDS("doc.campaignId") === result("campaignId") &&
            eventDS("doc.skillId") === result("skillId") &&
            eventDS("doc.engagementId") === result("engagementId") &&
            eventDS("doc.action") === result("Action")
        )

        // Group by the columns from the "result" DataFrame and select the first event for each group
        val firstEventForEachGroup = joinedDF.groupBy(
          result("campaignId"), result("skillId"), result("engagementId"), result("Action"), result("TotalActionCount")
        ).agg(
          first("modelName").as("firstModelName"),
          first("docKey").as("firstDocKey"),
          first("isUpdate").as("firstIsUpdate"),
          first("isUpsert").as("firstIsUpsert"),
          first("etlTime").as("firstEtlTime"),
          first("etlTimeHuman").as("firstEtlTimeHuman"),
          first("accountId").as("firstAccountId"),
          first("timeStamp").as("firstTimeStamp"),
          first("doc.rule").as("rule"),
          first("doc.timeInState").as("timeInState")
        )

        // Show the resulting DataFrame containing the first matching event for each group
        firstEventForEachGroup.show()

//              val datasetECEvent: Dataset[ECEvent] = firstEventForEachGroup.as[(String, String, String,String,Long, String, String, String,String, Long)].map {
//                case (campaignId, engagementId, skillId, action, totalActionCount) =>
//                  ECAggregate(campaignId, engagementId, skillId, action, totalActionCount)
//              }

        // Format and return the results
        //    val aggregatedData: List[(CampaignId, EngagementId, SkillId, Int)] = aggregatedEvents.collect().toList.map(row => (row(0), row(1), row(2), row(3)))
        result.show()
        println("OUTPUT EVENT: ")
        result.printSchema()
        //      firstEventForEachGroup
      } catch {
        case e: FileNotFoundException =>
          println("File not found or inaccessible: " + e.getMessage)
        case e: AnalysisException =>
          println("Error in schema or data  while reading intermediate JSON event: " + e.getMessage)
        case e: IOException =>
          println("IO Exception: " + e.getMessage)
        case e: SparkException =>
          println("Spark Exception: " + e.getMessage)
        case e: Exception =>
          println("Unexpected exception: " + e.getMessage)
        case t: Throwable =>
          println("An error occurred during processing of intermediate events while generating hourly reports due to: ", t)
      }
      ""



//    val results = teenagers.collect()
//
//    results.foreach(println)

    spark.stop()
  }
}

object Action {
  type Action = String

  val SHOW = "SHOW"
  val HIDE = "HIDE"
}

object EventType {
  type EventType = String

  val EngagementControllerReportingEvent = "EngagementControllerReportingEvent"
  //TODO: Add more event types based on future requirement.
}
