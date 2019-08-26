package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.api.java.UDF3

class GetByIndex extends UDF3[SparseVector, Int, Double, Double] {

  override def call(
                     v1: SparseVector,
                     index_to_find: Int,
                     default_value: Double
  ): Double = {
    for (i <- v1.indices.indices) {
      val index = v1.indices(i)
      if (index == index_to_find)
        return v1.values(i)
    }
    default_value
  }

}
