package com.clarify.array

import org.apache.spark.sql.api.java.UDF1

class Variance extends UDF1[Seq[Double], Double] {
  override def call(data: Seq[Double]): Double = {
    val avg = data.sum / data.length
    data.map(a => math.pow(a - avg, 2)).sum / data.length
  }
}
