package com.clarify.disruption.v2

import org.apache.spark.sql.api.java.UDF1

class Calculator extends UDF1[Seq[Double], Option[Double]] {
  override def call(data: Seq[Double]): Option[Double] = {

    val diff = for (i <- 1 until data.length)
      yield data(i) - data(i - 1)

    val index = diff.indices.maxBy(x => math.abs(x))

    diff.lift(index)

  }
}
