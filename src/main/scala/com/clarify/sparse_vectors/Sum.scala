package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.api.java.UDF2

class Sum extends UDF2[SparseVector, Double, Double] {

  override def call(v1: SparseVector, initial_value: Double): Double = {
    sparse_vector_sum(v1, initial_value)
  }

  def sparse_vector_sum(
      v1: SparseVector,
      initial_value: Double
  ): Double = {
      var sum: Double = initial_value
    for (i <- v1.indices.indices) {
      sum += v1.values(i)
    }
    sum
  }
}
