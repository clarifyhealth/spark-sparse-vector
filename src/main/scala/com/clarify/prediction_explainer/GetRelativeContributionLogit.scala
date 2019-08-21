package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vectors
import scala.collection.immutable.TreeMap
import scala.util.control.Breaks._
import org.apache.spark.sql.api.java.UDF4
import scala.util.control.Breaks._

class GetRelativeContributionLogit
    extends UDF4[SparseVector, SparseVector, Double, Double, SparseVector] {

  override def call(
      v1: SparseVector,
      v2: SparseVector,
      log_odds: Double,
      pop_log_odds: Double
  ): SparseVector = {
    sparse_calculate_relative_contribution_logit(v1, v2, log_odds, pop_log_odds)
  }

  def sparse_calculate_relative_contribution_logit(
      v1: SparseVector,
      v2: SparseVector,
      log_odds: Double,
      pop_log_odds: Double
  ): SparseVector = {
    val values: scala.collection.mutable.Map[Int, Double] =
      scala.collection.mutable.Map[Int, Double]()

    // first calculate eBx / eBX for feature that are set in v1
    for (i <- 0 until (v1.indices.size)) {
      // find the appropriate index on the other side
      val index = v1.indices(i)
      var division_factor: Double = Helpers.sparse_vector_get_float_by_index(v2, index, 0)
      // do the exponent divide
      val eBx: Double = Math.exp(v1.values(i))
      val eBX: Double = Math.exp(division_factor)
      val eBx_over_eBX: Double = eBx / eBX
      values(v1.indices(i)) = eBx_over_eBX
    }
    // secondly, calculate 1 / eBX for features that are not set in v1
    for (j <- 0 until (v2.indices.size)) {
      if(v1.indices.contains(v2.indices(j)) == false){
        val eBx: Double = 1
        val eBX: Double = Math.exp(v2.values(j))
        val eBx_over_eBX = eBx / eBX
        values(v2.indices(j)) = eBx_over_eBX
      }
    }

    // X1 + ... BnXn) and 1 + e^(B0 + B1x1 + ... Bnxn) once and
    //   then take the nth root and apply to each feature contribution
    // This is an approximation because the contribution of each feature
    //   will be slightly off since it is using the same value for 1 +e
    //   but the contributions will multiply together to be closer to the relative risk
    val number_features: Double = values.size
    val one_plus_e_sum_BX: Double = 1 + Math.exp(pop_log_odds) 
    val one_plus_e_sum_Bx: Double = 1 + Math.exp(log_odds)
    val one_plus_eBX_over_one_plus_eBx: Double = Math.pow((one_plus_e_sum_BX / one_plus_e_sum_Bx), (1 / number_features))
    // multiply all values with one_plus_eBX_over_one_plus_eBx
    for ((k,v) <- values) values(k) = values(k) * one_plus_eBX_over_one_plus_eBx
    return Vectors.sparse(v1.size, values.toSeq).asInstanceOf[SparseVector]
  }
}
