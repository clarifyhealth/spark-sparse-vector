package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.api.java.UDF1

class Product extends UDF1[SparseVector, Double] {

  override def call(v1: SparseVector): Double = {
    sparse_vector_product(v1)
  }

  def sparse_vector_product(
      v1: SparseVector
  ): Double = {
      var product: Double = 1
    for (i <- v1.indices.indices) {
      product = product * v1.values(i)
    }
    product
  }
}
