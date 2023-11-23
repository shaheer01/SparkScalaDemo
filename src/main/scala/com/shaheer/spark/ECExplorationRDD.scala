////package com.shaheer.spark
////
////import org.apache.spark.{SparkConf, SparkContext}
////import org.apache.spark.rdd.RDD
////import org.apache.avro.mapred.AvroKey
////import org.apache.avro.generic.GenericRecord
////import org.apache.hadoop.io.NullWritable
////import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
////import org.apache.hadoop.fs.Path
////
////object AvroRDDExample {
////  def main(args: Array[String]): Unit = {
////    val conf = new SparkConf().setAppName("AvroRDDExample")
////    val sc = new SparkContext(conf)
////
////    // Path to the Avro data files
////    val avroFilesPath = "data/stateevent/*.avro"
////
////    // Create a JobConf for reading Avro files
////    val jobConf = new JobConf()
////    FileInputFormat.setInputPaths(jobConf, new Path(avroFilesPath))
////
////    // Read Avro data into RDD
////    val avroRDD: RDD[(AvroKey[GenericRecord], NullWritable)] = sc.hadoopRDD(
////      jobConf,
////      classOf[org.apache.avro.mapred.AvroKeyInputFormat[GenericRecord]],
////      classOf[AvroKey[GenericRecord]],
////      classOf[NullWritable]
////    )
////
////    // Extract Avro data from the RDD
////    val avroData: RDD[GenericRecord] = avroRDD.map { case (key, _) => key.datum }
////
////    // Now you can work with the Avro data in the 'avroData' RDD
////
////    // For example, you can print the first few records
////    avroData.take(5).foreach(println)
////
////    // Don't forget to stop the SparkContext
////    sc.stop()
////  }
////}
//
//import org.apache.avro.Schema
//import org.apache.avro.file.DataFileReader
//import org.apache.avro.file.DataFileWriter
//import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
//import org.apache.avro.io.{DatumReader, DatumWriter}
//import org.apache.avro.Schema
//import org.apache.avro.file.DataFileReader
//import org.apache.avro.file.DataFileWriter
//import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
//import org.apache.avro.io.{DatumReader, DatumWriter}
//
//import java.io.{File, FileInputStream, FileOutputStream}
//import scala.collection.JavaConverters._
//import java.io.{File, FileInputStream, FileOutputStream}
//import java.util
//import scala.collection.convert.ImplicitConversions.{`iterable AsScalaIterable`, `list asScalaBuffer`}
//
//object AvroDataPreprocessor {
//
//  def main(args: Array[String]): Unit = {
//    // Define the Avro input and output file paths
//    val inputAvroFile = "data/stateevent/02.1695686566222.1695686534264.avro.EngagementStateDecisionEvent.application_1687673512210_710941_20230925200246_00032.c000.avro"
//    val outputAvroFile = "data/output.avro"
//
//    // Load the Avro data from the input file
//    val inputAvroData = loadAvroData(inputAvroFile)
//
//    // Modify the Avro schema to remove the problematic field
//    val modifiedAvroSchema = removeFieldFromSchema(inputAvroData.getSchema, "fieldToRemove")
//
//    // Create a new Avro DataFileWriter with the modified schema
//    val outputAvroDataWriter = createAvroDataWriter(outputAvroFile, modifiedAvroSchema)
//
//    // Process and write the modified records to the output file
//    inputAvroData.foreach { record =>
//      // Remove the problematic field from the record if needed
//      // record.put("fieldToRemove", null)
//
//      // Write the modified record to the output file
//      outputAvroDataWriter.append(record)
//    }
//
//    // Close the output Avro DataFileWriter
//    outputAvroDataWriter.close()
//  }
//
//  def loadAvroData(avroFilePath: String): DataFileReader[GenericRecord] = {
//    val avroFile = new File(avroFilePath)
//    val avroFileInputStream = new FileInputStream(avroFile)
//    val avroDataFileReader = new DataFileReader(avroFile, new GenericDatumReader[GenericRecord]())
//    avroDataFileReader
//  }
//
//  def createAvroDataWriter(avroFilePath: String, avroSchema: Schema): DataFileWriter[GenericRecord] = {
//    val avroFile = new File(avroFilePath)
//    val avroFileOutputStream = new FileOutputStream(avroFile)
//    val avroDataFileWriter = new DataFileWriter(new GenericDatumWriter[GenericRecord](avroSchema))
//    avroDataFileWriter.create(avroSchema, avroFileOutputStream)
//    avroDataFileWriter
//  }
//
//
////  def removeFieldFromSchema(schema: Schema, fieldNameToRemove: String): Schema = {
////    val fields = schema.getFields
////    val updatedFields = fields.filterNot(_.name() == fieldNameToRemove)
////    Schema createRecord(
////      schema.getName,
////      schema.getDoc,
////      schema.getNamespace,
////      schema.isError,
////      updatedFields
////    )
////  }
//def removeFieldFromSchema(schema: Schema, fieldNameToRemove: String): Schema = {
//  val fields = new util.ArrayList[Schema.Field](schema.getFields)
//  val updatedFields = fields.asScala.filterNot(_.name() == fieldNameToRemove).asJava
//  Schema.createRecord(
//    schema.getName,
//    schema.getDoc,
//    schema.getNamespace,
//    schema.isError,
//    updatedFields
//  )
//}
//}
