package com.shaheer.spark

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import java.io.{File, FileInputStream, FileOutputStream}
import scala.collection.JavaConverters._

object AvroDataPreprocessor {

  def main(args: Array[String]): Unit = {
    // Define the Avro input and output file paths
    val inputAvroFile = "input.avro"
    val outputAvroFile = "output.avro"

    // Load the Avro data from the input file
    val inputAvroData = loadAvroData(inputAvroFile)

    // Rename the "header" field to "newHeader" in the Avro schema
    val modifiedAvroSchema = renameFieldInSchema(inputAvroData.getSchema, "header", "newHeader")

    // Create a new Avro DataFileWriter with the modified schema
    val outputAvroDataWriter = createAvroDataWriter(outputAvroFile, modifiedAvroSchema)

    // Process and write the modified records to the output file
    while (inputAvroData.hasNext) {
      val record = inputAvroData.next()
      // Modify the renamed field if needed
      // record.put("newHeader", "ModifiedValue")

      // Write the modified record to the output file
      outputAvroDataWriter.append(record)
    }

    // Close the output Avro DataFileWriter
    outputAvroDataWriter.close()
  }

  def loadAvroData(avroFilePath: String): DataFileReader[GenericRecord] = {
    val avroFile = new File(avroFilePath)
    val avroFileInputStream = new FileInputStream(avroFile)
    val avroDataFileReader = new DataFileReader(avroFile, new GenericDatumReader[GenericRecord]())
    avroDataFileReader
  }

  def createAvroDataWriter(avroFilePath: String, avroSchema: Schema): DataFileWriter[GenericRecord] = {
    val avroFile = new File(avroFilePath)
    val avroFileOutputStream = new FileOutputStream(avroFile)
    val avroDataFileWriter = new DataFileWriter(new GenericDatumWriter[GenericRecord](avroSchema))
    avroDataFileWriter.create(avroSchema, avroFileOutputStream)
    avroDataFileWriter
  }

  def renameFieldInSchema(schema: Schema, oldFieldName: String, newFieldName: String): Schema = {
    val fields = schema.getFields.asScala.map { field =>
      if (field.name() == oldFieldName) {
        new Schema.Field(newFieldName, field.schema, field.doc(), field.defaultVal())
      } else {
        field
      }
    }
    Schema.createRecord(
      schema.getName,
      schema.getDoc,
      schema.getNamespace,
      schema.isError,
      fields.asJava
    )
  }
}
