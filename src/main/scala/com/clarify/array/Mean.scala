package com.clarify.array

import org.apache.spark.sql.api.java.UDF1

class Mean extends UDF1[Seq[Double], Double] {
  override def call(data: Seq[Double]): Double = {
    data.sum / data.length
  }
}
