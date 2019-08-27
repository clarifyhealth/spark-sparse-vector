package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.api.java.UDF2

class Divide
  extends UDF2[SparseVector, SparseVector, SparseVector] {

  override def call(v1: SparseVector, v2: SparseVector): SparseVector = {
    sparse_vector_divide(v1, v2)
  }

  def sparse_vector_divide(
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
      values(index) = v1.values(i) / division_factor
    }
    Vectors.sparse(v1.size, Helpers.remove_zeros(values).toSeq).asInstanceOf[SparseVector]
  }
}
