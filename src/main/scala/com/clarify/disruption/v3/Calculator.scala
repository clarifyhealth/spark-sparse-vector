package com.clarify.disruption.v3

import org.apache.spark.sql.api.java.UDF1

import scala.util.Try

class Calculator extends UDF1[Seq[Double], Option[Double]] {
  override def call(data: Seq[Double]): Option[Double] = {

    val diff = for (i <- 1 until data.length)
      yield data(i) - data(i - 1)

    val index = Try(diff.indices.minBy(x => x)).toOption

    val out = diff.lift(index.getOrElse(-1))

    Some(Array(out.getOrElse(0.0), 0.0).min)

  }
}
