package com.clarify.array

import breeze.stats._
import org.apache.spark.sql.api.java.UDF1

class Variance extends UDF1[Seq[Double], Double] {
  override def call(data: Seq[Double]): Double = {
    variance(data)
  }
}
