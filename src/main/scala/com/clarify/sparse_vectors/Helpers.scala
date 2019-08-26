package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SparseVector

object Helpers {
  def sparse_vector_get_float_by_index(
                                        v1: SparseVector,
                                        index_to_find: Int,
                                        default_value: Float
                                      ): Float = {
    for (i <- v1.indices.indices) {
      val index = v1.indices(i)
      if (index == index_to_find)
        return v1.values(i).toFloat
    }
    default_value
  }

  def sparse_vector_get_double_by_index(
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

  def remove_zeros(
                    values: scala.collection.mutable.Map[Int, Double]
                  ): scala.collection.mutable.Map[Int, Double] = {
    values.filter(v => v._2 != 0)
  }
}
