package com.clarify.example
import org.apache.spark.sql.api.java.UDF1

class WordCount extends UDF1[String, Long] {

  override def call(word: String): Long = {

    Option(word) match {
      case Some(tempStr) => tempStr.split(" ").length
      case None => 0
    }
  }

}
