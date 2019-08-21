package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vectors
import scala.util.control.Breaks._
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.api.java.UDF3

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

}
