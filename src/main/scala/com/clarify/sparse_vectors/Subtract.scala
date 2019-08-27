package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.api.java.UDF2

class Subtract extends UDF2[SparseVector, SparseVector, SparseVector] {

  override def call(v1: SparseVector, v2: SparseVector): SparseVector = {
    sparse_vector_subtract(v1, v2)
  }

  def sparse_vector_subtract(v1: SparseVector, v2: SparseVector): SparseVector = {
    require(v1.size == v2.size)
    val values: scala.collection.mutable.Map[Int, Double] =
      scala.collection.mutable.Map[Int, Double]()
    // Add values from v1
    for (i <- v1.indices.indices) {
      val index = v1.indices(i)
      values(index) =
        if (values.contains(index)) values(index) + v1.values(i)
        else v1.values(i)
    }
    // Add values from v2
    for (i <- v2.indices.indices) {
      val index = v2.indices(i)
      values(index) =
        if (values.contains(index)) values(index) - v2.values(i)
        else 0 - v2.values(i)
    }
    Vectors.sparse(v1.size, Helpers.remove_zeros(values).toSeq).asInstanceOf[SparseVector]
  }
}
