package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.api.java.UDF3

class GetByIndex extends UDF3[SparseVector, Int, Double, Double] {

  override def call(
      v1: SparseVector,
      index: Int,
      default_value: Double
  ): Double = {
    Helpers.sparse_vector_get_float_by_index(v1, index, default_value)
  }

}
