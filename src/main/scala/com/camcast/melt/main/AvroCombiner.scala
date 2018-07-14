package com.camcast.melt.main

import java.io.File

import com.camcast.melt.common.{HWMDateFormat, HWMUtil, Log}
import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

/**
 * Created by tprabh001c on 5/1/18.
 */
object AvroCombiner extends Serializable {

  Logger.getLogger("org").setLevel(Level.WARN)

  /**
   * Main method
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    Log.info("Starting des_spark Driver...")
    val config = ConfigFactory.load("qa/des_formatter.conf")
    val hwmFilePath = config.getString("des.formatter.hwm.path")
    val runInterval = config.getString("des.formatter.interval").toInt
    val appName = config.getString("app.name")

    val spark = SparkSession.builder.appName(appName).getOrCreate

    val sparkConfig: Configuration = spark.sparkContext.hadoopConfiguration
    val offset = spark.read.text(s"$hwmFilePath/part-*")


    val file = new File(config.getString("des.formatter.avro.schema"))
    val parser = new Parser()

    val schema = parser.parse(file);

    sparkConfig.set("avro.schema.input.key", schema.toString)
    sparkConfig.set("avro.schema.output.key", schema.toString)
    val offValue = offset.first().getString(0)
    Log.info(s"initial offset : $offValue")
    val inputHWM = new Gson().fromJson(offValue, classOf[HWMDateFormat])
    val dtTime = HWMUtil.createDateTimeFromHWM(inputHWM)
    val isHighWaterMarkReached = HWMUtil.isHighWaterMarkReached(dtTime, runInterval)
    if (!isHighWaterMarkReached) {
      Log.info("High water mark not reached. Streaming data.")
      streamData(spark, inputHWM, hwmFilePath, config, sparkConfig)
      Log.info("Streaming completed. Updating high water mark")
      val outFormat = HWMUtil.updateOffset(spark, offset, inputHWM, hwmFilePath, runInterval)
      Log.info(s"Offset updated $outFormat")
    } else {
      Log.info("High water mark reached. Streaming data skipped.")
    }
  }

  /**
   * Spark streaming method
   *
   * @param spark
   * @param inputHWM
   * @param hwmFilePath
   */
  def streamData(spark: SparkSession,
                 inputHWM: HWMDateFormat,
                 hwmFilePath: String,
                 config:Config,
                 sparkConfig:Configuration): Unit = {
    val outputPath = config.getString("des.formatter.output.path")
    val coalesceNum = config.getInt("des.coalescenum")
    val outputFormat = config.getString("des.formatter.output.format")
    val compression = config.getString("des.formatter.output.compression")
    val inputPath = config.getString("des.formatter.input.path")
    val inRegex = config.getString("des.formatter.input.regex")
    val dateStr = HWMUtil.getOffsetDateStr(inputHWM)
    val inputXreApplication = inputPath + "2018/07/14/00/*.avro"

    val rdd = spark.sparkContext.newAPIHadoopFile(
      inputXreApplication,
      classOf[AvroKeyInputFormat[GenericRecord]],
      classOf[AvroKey[GenericRecord]],
      classOf[NullWritable],
      sparkConfig
    )

    rdd
      .coalesce(1)
      .saveAsNewAPIHadoopFile(
        outputPath,
        classOf[AvroKey[GenericRecord]],
        classOf[NullWritable],
        classOf[AvroKeyOutputFormat[GenericRecord]],
        sparkConfig
      )
  }
}
