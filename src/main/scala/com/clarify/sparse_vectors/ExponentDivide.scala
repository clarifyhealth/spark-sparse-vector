package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.api.java.UDF2

class ExponentDivide
  extends UDF2[SparseVector, SparseVector, SparseVector] {

  override def call(v1: SparseVector, v2: SparseVector): SparseVector = {
    sparse_vector_exponent_divide(v1, v2)
  }

  def sparse_vector_exponent_divide(
                                     v1: SparseVector,
                                     v2: SparseVector
                                   ): SparseVector = {
    require(v1.size == v2.size)
    val values: scala.collection.mutable.Map[Int, Double] =
      scala.collection.mutable.Map[Int, Double]()
    // Add values from v1
    for (i <- v1.indices.indices) {
      val index = v1.indices(i)
      var division_factor: Double = 0
      for (j <- v2.indices.indices) {
        if (v2.indices(j) == v1.indices(i)) {
          division_factor = v2.values(j)
        }
      }
      values(index) = Math.exp(v1.values(i)) / Math.exp(division_factor)
    }
    // now add when value in v1 is zero
    for (j <- v2.indices.indices) {
      val index = v2.indices(j)
      if (!v1.indices.contains(v2.indices(j))) {
        values(index) = 1 / Math.exp(v2.values(j))
      }
    }
    // now find values where both v1 and v2 are zero and set those to 1
    for (z <- 0 until v1.size) {
      if (!values.exists(_._1 == z)) {
        values(z) = 1
      }
    }
    Vectors.sparse(v1.size, Helpers.remove_zeros(values).toSeq).asInstanceOf[SparseVector]
  }
}
