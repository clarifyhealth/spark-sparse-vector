package com.clarify.hdfs

import com.clarify.Helpers
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.{SparkContext, SparkException}

import scala.sys.process._

object HdfsHelper {
  def _printFreeSpace(sparkContext: SparkContext): Boolean = {
    val deployMode: String = sparkContext.getConf.get("spark.submit.deployMode", null)
    if (deployMode != null && deployMode != "client") {
      //noinspection SpellCheckingInspection
      val results = Seq("hdfs", "dfs", "-df", "-h").!!.trim
      Helpers.log(results)
    }
    else {
      Helpers.log("Skipped showing free space since running in client mode")
    }
  }

  def __folderWithDataExists(sql_ctx: SQLContext, location: String, name: String): Boolean = {
    try {
      if (name != null) {
        sql_ctx.sparkContext.setJobDescription(f"$name (check if folder exists)")
      }
      sql_ctx.read.parquet(location).take(1)
      true
    }
    catch {
      case e: SparkException =>
        val cause = e.getCause
        Helpers.log(s"__folderExists: Got SparkException: $cause")
        throw cause
      case e: AnalysisException =>
        // we do this instead of checking if data frame is empty because the latter is expensive
        if (e.message.startsWith(s"Unable to infer schema for Parquet. It must be specified manually.")) {
          Helpers.log(s"__folderExists: data frame passed in is empty $location. $e")
          false
        }
        else if (e.message.startsWith("Path does not exist")) {
          Helpers.log(s"__folderExists: path does not exist $location. $e")
          false
        }
        else {
          Helpers.log(s"__folderExists: Got AnalysisException: $e")
          throw e
        }
      case unknown: Throwable =>
        Helpers.log(s"__folderExists: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  def appendPaths(path1: String, path2: String): String = {
    addTrailingSlash(path1) + path2
  }

  private def addTrailingSlash(path: String): String = {
    if (!path.endsWith("/")) return path + "/"
    path
  }

  def getFoldersWithPrefix(sparkContext: SparkContext, path: String, prefix: String): Seq[String] = {
    val deployMode = sparkContext.getConf.get("spark.submit.deployMode", null)
    if (deployMode != null && deployMode != "client") {
      //noinspection SpellCheckingInspection
      val results: String = Seq("hdfs", "dfs", "-ls", "-C", path).!!.trim
      results.split("\\s+").toSeq.filter(folder => folder.startsWith(prefix))
    }
    else {
      // use local file system calls
      val results: String = Seq("ls", "-C", path).!!.trim
      results.split("\\s+").toSeq.filter(folder => folder.startsWith(prefix))
    }
  }

  def getFolderNumbersOnly(sparkContext: SparkContext, path: String, prefix: String): Seq[Int] = {
    val list_of_folders = getFoldersWithPrefix(sparkContext, path, prefix)
    list_of_folders.map(f => f.replace(f"$prefix", "").toInt)
  }

  def getLatestFolderNumber(sparkContext: SparkContext, path: String, prefix: String): Option[Int] = {
    getFolderNumbersOnly(sparkContext, path, prefix).sorted.reverse.headOption
  }

  def s3distCp(src: String, dest: String): Unit = {
    s"s3-dist-cp --src $src --dest $dest".!
  }
}
