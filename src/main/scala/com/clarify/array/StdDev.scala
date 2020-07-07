package com.clarify.array

import breeze.stats._
import org.apache.spark.sql.api.java.UDF1

class StdDev extends UDF1[Seq[Double], Double] {
  override def call(data: Seq[Double]): Double = {
    // https://github.com/scalanlp/breeze/issues/507
    stddev(data) * Math.pow((data.length - 1) / data.length, .5)
  }
}
