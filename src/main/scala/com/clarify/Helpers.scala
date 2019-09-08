package com.clarify

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.collection.JavaConverters

object Helpers {

  import java.util

  def getSeqString(list: util.ArrayList[String]): Seq[String] = JavaConverters.asScalaIteratorConverter(list.listIterator()).asScala.toSeq

  def log(message: String): Boolean = {
    //    val logger = _LOGGER
    //    logger.info(message)
    println(s"$getCurrentDateTimeStamp [Scala] $message")
    true
  }

  def getCurrentDateTimeStamp: String = {
    LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss.ms"))
  }
}
