package com.clarify

import java.io.File
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

  def getListOfFiles(dir: String): Seq[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  /**
   * Get a recursive listing of all files underneath the given directory.
   * from stackoverflow.com/questions/2637643/how-do-i-list-all-files-in-a-subdirectory-in-scala
   */
  def printRecursiveListOfFiles(dir: File): Unit = {
    val files = dir.listFiles
    for (file <- files) {
      if (file.isDirectory) {
        println(f">${file.getName}")
        printRecursiveListOfFiles(file)
      }
      else {
        println(file.getName)
      }
    }
  }
}
