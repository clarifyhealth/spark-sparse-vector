package com.clarify.signal

import org.apache.spark.sql.api.java.UDF2
import breeze.signal._
import breeze.linalg._

class Convolve extends UDF2[Seq[Double], Seq[Double], Seq[Double]] {

  override def call(data: Seq[Double], kernel: Seq[Double]): Seq[Double] = {
    convolve(DenseVector(data: _*), DenseVector(kernel: _*)).toArray
  }

}
