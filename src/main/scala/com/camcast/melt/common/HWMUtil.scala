package com.camcast.melt.common

import java.io.FileInputStream
import java.util.Properties

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.{DateTime, DateTimeZone}

/**
 * Created by tprabh001c on 5/1/18.
 */
object HWMUtil {

  val Log = Logger.getLogger("HWMUtil")
  Log.setLevel(Level.DEBUG)

  /**
   * Get date string from high water mark
   *
   * @param dtFormat
   * @return
   */
  def getOffsetDateStr(dtFormat: HWMDateFormat): String = {
    return dtFormat.year + "/" + dtFormat.month + "/" + dtFormat.day + "/"
  }

  /**
   * Update high water mark file
   *
   * @param spark
   * @param rawData
   * @param file_path
   */
  def updateHWMFile(spark: SparkSession, rawData: List[String], file_path: String): Unit = {
    import spark.implicits._
    rawData
      .toDF
      .write
      .mode(SaveMode.Overwrite)
      .text(file_path)
  }

  /**
   * Load config file
   *
   * @param configPath
   * @return
   */
  def loadConfigFile(configPath: String): Properties = {
    Log.info("Loading configuration from: " + configPath)
    val prop = new Properties()
    try {
      prop.load(new FileInputStream(configPath))
    } catch {
      case e: Exception =>
        Log.error(e.getMessage)
    }
    return prop
  }

  /**
   * Create object from high water mark
   *
   * @param dtFormat
   * @return
   */
  def createDateTimeFromHWM(dtFormat: HWMDateFormat): DateTime = {
    var offsetDT = new DateTime().withZone(DateTimeZone.UTC)
    offsetDT = offsetDT.withYear(dtFormat.year.toInt)
      .withMonthOfYear(dtFormat.month.toInt)
      .withDayOfMonth(dtFormat.day.toInt)
      .withHourOfDay(dtFormat.hour.toInt)
    return offsetDT
  }

  /**
   * Calculate next high water mark and update high water mark
   *
   * @param spark
   * @param offset
   * @param dtFormat
   * @param hwmFilePath
   * @return
   */
  def updateOffset(spark: SparkSession, offset: DataFrame, dtFormat: HWMDateFormat, hwmFilePath: String,
    runInterval: Int): HWMDateFormat = {
    var offsetDT = createDateTimeFromHWM(dtFormat)
    val outFormat = new HWMDateFormat()
    val nf = java.text.NumberFormat.getIntegerInstance(java.util.Locale.US)
    nf.setMinimumIntegerDigits(2)
    var isUpdateRequired = false
    if (!isHighWaterMarkReached(offsetDT, runInterval)) {
      offsetDT = offsetDT.plusHours(getHourlyIncrement(runInterval))
      val hrOfDay = nf.format(offsetDT.getHourOfDay)
      val dayOfMonth = nf.format(offsetDT.getDayOfMonth)
      val mOfyear = nf.format(offsetDT.getMonthOfYear)
      val year = offsetDT.getYear.toString
      val mapHrRange = getHourlyMap(runInterval)
      outFormat.hourRange = mapHrRange(hrOfDay)
      isUpdateRequired = true
    } else {
      outFormat.hourRange = dtFormat.hourRange
    }
    outFormat.year = offsetDT.getYear().toString
    outFormat.month = nf.format(offsetDT.getMonthOfYear)
    outFormat.day = nf.format(offsetDT.getDayOfMonth)
    outFormat.hour = nf.format(offsetDT.getHourOfDay)
    if (isUpdateRequired) {
      val gson = new Gson()
      val outputHWM = gson.toJson(outFormat, classOf[HWMDateFormat])
      updateHWMFile(spark, List(outputHWM), hwmFilePath)
      Log.info("Offset updated : " + outputHWM)
    } else {
      Log.info("Offset update not required. Already up to date")
    }
    return outFormat
  }

  def getHourlyIncrement(runInterval: Int): Int = {
    var result = runInterval match {
      case `hourlyIncrement2` => hourlyIncrement2
      case `hourlyIncrement4` => hourlyIncrement4
      case `hourlyIncrement6` => hourlyIncrement6
      case `hourlyIncrement8` => hourlyIncrement8
    }
    return result
  }

  def getHourlyMap(runInterval: Int): Map[String, String] = {
    var result = runInterval match {
      case `hourlyIncrement2` => mapHrRange2
      case `hourlyIncrement4` => mapHrRange4
      case `hourlyIncrement6` => mapHrRange6
      case `hourlyIncrement8` => mapHrRange8
    }
    return result
  }

  /**
   * Check if high water mark reached.
   *
   * @param offsetDT
   * @return
   */
  def isHighWaterMarkReached(offsetDT: DateTime, runInterval: Int): Boolean = {
    var isHighWaterMarkReached = false
    var today = new DateTime().withZone(DateTimeZone.UTC)
    val hwm = -(runInterval + 2)
    today = today.plusHours(hwm)
    Log.info("Today " + hwm + " hr = " + today)
    val cmp = offsetDT.compareTo(today)
    if (cmp < 0) {
      isHighWaterMarkReached = false
      Log.info("Incrementing water mark")
    } else {
      isHighWaterMarkReached = true
      Log.info("High water mark already reached")
    }
    return isHighWaterMarkReached
  }

  val mapHrRange2 = Map(
    "00" -> "{00,01}",
    "02" -> "{02,03}",
    "04" -> "{04,05}",
    "06" -> "{06,07}",
    "08" -> "{08,09}",
    "10" -> "{10,11}",
    "12" -> "{12,13}",
    "14" -> "{14,15}",
    "16" -> "{16,17}",
    "18" -> "{18,19}",
    "20" -> "{20,21}",
    "22" -> "{22,23}")

  val hourlyIncrement2 = 2

  val mapHrRange4 = Map(
    "00" -> "{00,01,02,03}",
    "04" -> "{04,05,06,07}",
    "08" -> "{08,09,10,11}",
    "12" -> "{12,13,14,15}",
    "16" -> "{16,17,18,19}",
    "20" -> "{20,21,22,23}")

  val hourlyIncrement4 = 4

  val mapHrRange6 = Map(
    "00" -> "{00,01,02,03,04,05}",
    "06" -> "{06,07,08,09,10,11}",
    "12" -> "{12,13,14,15,16,17}",
    "18" -> "{18,19,20,21,22,23}")

  val hourlyIncrement6 = 6

  val mapHrRange8 = Map(
    "00" -> "{00,01,02,03,04,05,06,07}",
    "08" -> "{08,09,10,11,12,13,14,15}",
    "16" -> "{16,17,18,19,20,21,22,23}")

  val hourlyIncrement8 = 8
}
