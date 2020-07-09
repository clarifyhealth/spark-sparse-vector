package com.clarify.array

import org.apache.spark.sql.api.java.UDF1

class StdDev extends UDF1[Seq[Double], Double] {
  override def call(data: Seq[Double]): Double = {
    val avg = data.sum / data.length
    val variance = data.map(x => math.pow(x - avg, 2)).sum / data.length
    math.sqrt(variance)
  }
}
