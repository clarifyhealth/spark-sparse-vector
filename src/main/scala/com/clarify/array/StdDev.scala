package com.clarify.array

import breeze.stats._
import org.apache.spark.sql.api.java.UDF1

class StdDev extends UDF1[Seq[Double], Double] {
  override def call(data: Seq[Double]): Double = {
    stddev(data)
  }
}
