package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.api.java.UDF1
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.api.java.UDF2
import scala.collection.immutable.TreeMap
import scala.util.control.Breaks._

class SparseVectorExponentDivide
    extends UDF2[SparseVector, SparseVector, SparseVector] {

  override def call(v1: SparseVector, v2: SparseVector): SparseVector = {
    sparse_vector_exponent_divide(v1, v2)
  }

  def sparse_vector_exponent_divide(
      v1: SparseVector,
      v2: SparseVector
  ): SparseVector = {
    val values: scala.collection.mutable.Map[Int, Double] =
      scala.collection.mutable.Map[Int, Double]()
    // Add values from v1
    for (i <- 0 until (v1.indices.size)) {
      val index = v1.indices(i)
      var division_factor: Double = 0
      for (j <- 0 until (v2.indices.size)) {
        if (v2.indices(j) == v1.indices(i)) {
          division_factor = v2.values(j)
          break
        }
      }
      values(index) = Math.exp(v1.values(i)) / Math.exp(division_factor)
    }
    for (i <- 0 until (v2.indices.size)) {
      val index = v2.indices(i)
      values(index) = 1 / Math.exp(v2.values(i))
    }
    return Vectors.sparse(v1.size, Helpers.remove_zeros(values).toSeq).asInstanceOf[SparseVector]
  }
}
