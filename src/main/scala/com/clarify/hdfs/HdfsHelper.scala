package com.clarify.hdfs

import java.nio.file.Paths

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
    if (name != null) {
      sql_ctx.sparkContext.setJobDescription(f"$name (check if folder exists)")
    }
    _folderWithDataExistsInternal(sql_ctx, location)
  }

  private def _folderWithDataExistsInternal(sql_ctx: SQLContext, location: String): Boolean = {
    try {
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
    Paths.get(path1, path2).toString
  }

  def getFoldersWithPrefix(sparkContext: SparkContext, sql_ctx: SQLContext, path: String, prefix: String): Seq[String] = {
    val deployMode = sparkContext.getConf.get("spark.submit.deployMode", null)
    if (deployMode != null && deployMode != "client") {
      //noinspection SpellCheckingInspection
      // create folder if it doesn't exist
      Seq("hdfs", "dfs", "-mkdir", "-p", path).!!.trim
      val results: String = Seq("hdfs", "dfs", "-ls", "-C", path).!!.trim
      results.split("\\s+").toSeq.filter(folder => folder.startsWith(prefix))
        .filter(r => _folderWithDataExistsInternal(sql_ctx = sql_ctx, location = Paths.get(path, r).toString))
    }
    else {
      // use local file system calls
      Seq("mkdir", "-p", path).!!.trim
      val results: String = Seq("ls", "-C", path).!!.trim
      results.split("\\s+").toSeq.filter(folder => folder.startsWith(prefix))
        .filter(r => _folderWithDataExistsInternal(sql_ctx = sql_ctx, location = Paths.get(path, r).toString))
    }
  }

  def getFolderNumbersOnly(sparkContext: SparkContext, sql_ctx: SQLContext, path: String, prefix: String): Seq[Int] = {
    val list_of_folders = getFoldersWithPrefix(sparkContext, sql_ctx, path, prefix)
    list_of_folders.map(f => f.replace(f"$prefix", ""))
      .filter(r => r.forall(c => c.isDigit))
      .map(r => r.toInt)
  }

  def getLatestFolderNumber(sparkContext: SparkContext, sql_ctx: SQLContext, path: String, prefix: String): Option[Int] = {
    getFolderNumbersOnly(sparkContext, sql_ctx, path, prefix).sorted.reverse.headOption
  }

  def s3distCp(src: String, dest: String): Unit = {
    s"s3-dist-cp --src $src --dest $dest".!
  }
}
