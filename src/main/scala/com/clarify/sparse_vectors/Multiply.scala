package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.api.java.UDF1
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.api.java.UDF2
import scala.collection.immutable.TreeMap
import scala.util.control.Breaks._

class SparseVectorMultiply
    extends UDF2[SparseVector, SparseVector, SparseVector] {

  override def call(v1: SparseVector, v2: SparseVector): SparseVector = {
    sparse_vector_multiply(v1, v2)
  }

  def sparse_vector_multiply(
      v1: SparseVector,
      v2: SparseVector
  ): SparseVector = {
    val values: scala.collection.mutable.Map[Int, Double] =
      scala.collection.mutable.Map[Int, Double]()
    // Add values from v1
    for (i <- 0 until (v1.indices.size)) {
      val index = v1.indices(i)
      var multiply_factor: Double = 0
      for (j <- 0 until (v2.indices.size)) {
        if (v2.indices(j) == v1.indices(i)) {
          multiply_factor = v2.values(j)
          break
        }
      }
      values(index) = v1.values(i) * multiply_factor
    }
    return Vectors.sparse(v1.size, values.toSeq).asInstanceOf[SparseVector]
  }
}
