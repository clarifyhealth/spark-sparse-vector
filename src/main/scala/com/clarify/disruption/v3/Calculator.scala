package com.clarify.disruption.v3

import org.apache.spark.sql.api.java.UDF1

import scala.util.Try

class Calculator extends UDF1[Seq[Double], Option[Double]] {
  override def call(data: Seq[Double]): Option[Double] = {

    Option(data) match {
      case Some(value: Seq[Double]) =>
        if (value.exists(_.isNaN)) {
          Some(0.0)
        }
        else {
          // calculate the delta for each reading (subtracting the previous entry from it)
          val diff: IndexedSeq[Double] = for (i <- 1 until value.length)
            yield value(i) - value(i - 1)

          // find index of element with the minimum value
          //    val index = Try(diff.indices.minBy(x => x)).toOption
          val index: Option[Int] = Try(diff.indices.minBy(diff)).toOption

          // use value or -1 if there is no value
          val out: Option[Double] = diff.lift(index.getOrElse(-1))

          // change any value greater than 0 to 0 so we get only negative disruption
          Some(Array(out.getOrElse(0.0), 0.0).min)
        }
      case None =>
        Some(0.0)
    }
  }
}
