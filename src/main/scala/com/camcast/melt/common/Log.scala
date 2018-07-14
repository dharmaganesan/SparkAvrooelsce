package com.camcast.melt.common

import java.io.Serializable

import org.apache.log4j.Logger

/**
 * Created by tprabh001c on 5/1/18.
 */
object Log extends Serializable {

  val logger = Logger.getLogger("des_formatter")

  def info(msg: String): Unit = {
    logger.info(msg)
  }

  def warn(msg: String): Unit = {
    logger.warn(msg)
  }

  def error(msg: String): Unit = {
    logger.error(msg)
  }
}
