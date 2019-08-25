package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SparseVector

object Helpers {
  def sparse_vector_get_float_by_index(
      v1: SparseVector,
      index_to_find: Int,
      default_value: Double
  ): Double = {
    for (i <- 0 until (v1.indices.size)) {
      val index = v1.indices(i)
      if (index == index_to_find)
        return v1.values(i)
    }
    return default_value
  }

  def get_feature_name(
                        feature_list: Seq[FeatureListItem],
                        index: Int
  ): String = {
    feature_list.filter(x => x.feature_index == index).head.feature_name
  }

  def remove_zeros(
    values: scala.collection.mutable.Map[Int, Double]
  ): scala.collection.mutable.Map[Int, Double] = {
    values.filter(v => v._2 != 0)
  }
}
