package com.clarify

import scala.collection.JavaConverters

object Helpers {

  import java.util

  def getSeqString(list: util.ArrayList[String]): Seq[String] = JavaConverters.asScalaIteratorConverter(list.listIterator()).asScala.toSeq
}
