package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.api.java.UDF1
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vectors

class SparseVectorAdd extends UDF1[SparseVector, SparseVector] {

  override def call(v1: SparseVector): SparseVector = {

    Option(v1) match {
      case Some(v1) => sparse_vector_add(v1, v1)
      case None     => new SparseVector(3, Array(0), Array(0.1))
    }
  }

  def sparse_vector_add(v1: SparseVector, v2: SparseVector): SparseVector = {
    val values: scala.collection.mutable.Map[Int, Double] =
      scala.collection.mutable.Map[Int, Double]()
    // Add values from v1
    for (i <- 0 until (v1.indices.size)) {
      val index = v1.indices(i)
      values(index) =
        if (values.contains(index)) values(index) + v1.values(i)
        else v1.values(i)
    }
    // Add values from v2
    for (i <- 0 until (v2.indices.size)) {
      val index = v2.indices(i)
      values(index) =
        if (values.contains(index)) values(index) + v2.values(i)
        else v2.values(i)
    }
    return new SparseVector(v1.size, values.keys.toArray, values.values.toArray)
  }
}
