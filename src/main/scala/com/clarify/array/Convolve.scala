package com.clarify.array

import breeze.linalg._
import breeze.signal._
import org.apache.spark.sql.api.java.UDF2

class Convolve extends UDF2[Seq[Double], Int, Seq[Double]] {

  override def call(data: Seq[Double], window: Int): Seq[Double] = {
    val kernel = 1 to window map (_ => 1.0 / window)
    convolve(DenseVector(data: _*), DenseVector(kernel: _*)).toArray
  }
}
