//package com.shaheer.spark
//
//import org.apache.avro.Schema
//import org.apache.avro.file.{DataFileReader, DataFileWriter}
//import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.SparkSession
//import org.apache.avro.Schema
//import org.apache.avro.SchemaBuilder
//
//import java.io.{File, FileInputStream, FileOutputStream}
//import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
//import scala.jdk.CollectionConverters.asScalaBufferConverter
//
//object ECExploration {
//
//
//    def main(args: Array[String]): Unit = {
//      // Define the Avro input and output file paths
//      val inputAvroFile = "input.avro"
//      val outputAvroFile = "output.avro"
//
//      // Load the Avro data from the input file
//      val inputAvroData = loadAvroData(inputAvroFile)
//
//      // Modify the Avro schema to remove the problematic field
//      val modifiedAvroSchema = removeFieldFromSchema(inputAvroData.getSchema, "fieldToRemove")
//
//      // Create a new Avro DataFileWriter with the modified schema
//      val outputAvroDataWriter = createAvroDataWriter(outputAvroFile, modifiedAvroSchema)
//
//      // Process and write the modified records to the output file
//      inputAvroData.foreach { record =>
//        // Remove the problematic field from the record if needed
//        // record.put("fieldToRemove", null)
//
//        // Write the modified record to the output file
//        outputAvroDataWriter.append(record)
//      }
//
//      // Close the output Avro DataFileWriter
//      outputAvroDataWriter.close()
//    }
//
//    def loadAvroData(avroFilePath: String): DataFileReader[GenericRecord] = {
//      val avroFile = new File(avroFilePath)
//      val avroFileInputStream = new FileInputStream(avroFile)
//      val avroDataFileReader = new DataFileReader(avroFile, new GenericDatumReader[GenericRecord]())
//      avroDataFileReader
//    }
//
//    def createAvroDataWriter(avroFilePath: String, avroSchema: Schema): DataFileWriter[GenericRecord] = {
//      val avroFile = new File(avroFilePath)
//      val avroFileOutputStream = new FileOutputStream(avroFile)
//      val avroDataFileWriter = new DataFileWriter(new GenericDatumWriter[GenericRecord](avroSchema))
//      avroDataFileWriter.create(avroSchema, avroFileOutputStream)
//      avroDataFileWriter
//    }
//
//  def removeFieldFromSchema(schema: Schema, fieldNameToRemove: String): Schema = {
//    val fields = schema.getFields.asScala
//    val updatedFields = fields.filterNot(_.name() == fieldNameToRemove).asJava
//    Schema.createRecord(
//      schema.getName,
//      schema.getDoc,
//      schema.getNamespace,
//      schema.isError,
//      updatedFields
//    )
//  }
//
//
//  /*
//    def main(args: Array[String]): Unit = {
//
//      Logger.getLogger("org").setLevel(Level.ERROR)
//
//
//      val spark = SparkSession
//        .builder
//        .appName("SparkSQL")
//        .master("local[*]")
//        .getOrCreate()
//
//
//      import spark.implicits._
//      val decisionEvents = spark.read
//  //      .option("header", "true")
//  //      .option("inferSchema", "true")
//        .format("avro")
//        .load("data/stateevent/02.1695686566222.1695686534264.avro.EngagementStateDecisionEvent.application_1687673512210_710941_20230925200246_00032.c000.avro")
//  //      .as[DecisionEvent] ///DS
//
//      decisionEvents.printSchema()
//      decisionEvents.show()
//
//      //Stop the SparkSession
//      spark.stop()
//    }*/
//
//}
