package com.clarify.prediction_explainer

import com.clarify.sparse_vectors.Helpers
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.api.java.UDF5

class CalculateRelativeContribution
  extends UDF5[String, SparseVector, SparseVector, Double, Double, SparseVector] {

  override def call(
                     link: String,
                     v1: SparseVector,
                     v2: SparseVector,
                     log_odds: Double,
                     pop_log_odds: Double
                   ): SparseVector = {
    sparse_calculate_relative_contribution(link, v1, v2, log_odds, pop_log_odds)
  }

  /**
   * For each element x(i) in v1 and y(i) in v2, return e^x(i)/e^y(i) * (pop_log_odds/row_log_odds) to (1/n)
   *
   * @param row_log_odds_contribution_vector row_log_odds_contribution_vector
   * @param population_log_odds_vector       population_log_odds_vector
   * @param row_log_odds                     row_log_odds
   * @param pop_log_odds                     pop_log_odds
   * @return
   */
  def sparse_calculate_relative_contribution(link: String,
                                             row_log_odds_contribution_vector: SparseVector,
                                             population_log_odds_vector: SparseVector,
                                             row_log_odds: Double,
                                             pop_log_odds: Double
                                            ): SparseVector = {

    link match {
      case "logit" => sparse_calculate_relative_contribution_logit(row_log_odds_contribution_vector,
        population_log_odds_vector,
        row_log_odds,
        pop_log_odds
      )
      case "log" => sparse_calculate_relative_contribution_log(row_log_odds_contribution_vector,
        population_log_odds_vector,
        row_log_odds,
        pop_log_odds
      )
      case _ => throw new IllegalArgumentException(s"link function [$link] is not supported")
    }
  }

  /**
   * For each element x(i) in v1 and y(i) in v2, return e^x(i)/e^y(i) * (pop_log_odds/row_log_odds) to (1/n)
   *
   * @param row_log_odds_contribution_vector row_log_odds_contribution_vector
   * @param population_log_odds_vector       population_log_odds_vector
   * @param row_log_odds                     row_log_odds
   * @param pop_log_odds                     pop_log_odds
   * @return
   */
  def sparse_calculate_relative_contribution_logit(row_log_odds_contribution_vector: SparseVector,
                                                   population_log_odds_vector: SparseVector,
                                                   row_log_odds: Double,
                                                   pop_log_odds: Double
                                                  ): SparseVector = {
    //     For each element x(i) in v1 and y(i) in v2,
    //     return e^x(i)/e^y(i)  * nth root of 1 + e^(B0 + B1X1 + ... BnXn)/ 1 + e^(B0 + B1x1 + ... Bnxn)
    // :param v1: current row's log odds vector
    //        [ 0.1, 0.2, 0.3 ] means B1x1 = 0.1, B2x2 = 0.2, B2x2 = 0.3
    // :param v2: population log odds vector
    //        [0.1, 0.2, 0.3] means B1X1 = 0.1, B2X2 = 0.2, B3X3 = 0.3

    val values: scala.collection.mutable.Map[Int, Double] =
      scala.collection.mutable.Map[Int, Double]()

    // first calculate eBx / eBX for feature that are set in v1
    for (i <- row_log_odds_contribution_vector.indices.indices) {
      // find the appropriate index on the other side
      val index = row_log_odds_contribution_vector.indices(i)
      val division_factor: Double = Helpers.sparse_vector_get_float_by_index(population_log_odds_vector, index, 0)
      // do the exponent divide
      val eBx: Double = Math.exp(row_log_odds_contribution_vector.values(i))
      val eBX: Double = Math.exp(division_factor)
      val eBx_over_eBX: Double = eBx / eBX
      values(row_log_odds_contribution_vector.indices(i)) = eBx_over_eBX
    }
    // secondly, calculate 1 / eBX for features that are not set in v1
    for (j <- population_log_odds_vector.indices.indices) {
      if (!row_log_odds_contribution_vector.indices.contains(population_log_odds_vector.indices(j))) {
        val eBx: Double = 1
        val eBX: Double = Math.exp(population_log_odds_vector.values(j))
        val eBx_over_eBX = eBx / eBX
        values(population_log_odds_vector.indices(j)) = eBx_over_eBX
      }
    }

    // X1 + ... BnXn) and 1 + e^(B0 + B1x1 + ... Bnxn) once and
    //   then take the nth root and apply to each feature contribution
    // This is an approximation because the contribution of each feature
    //   will be slightly off since it is using the same value for 1 +e
    //   but the contributions will multiply together to be closer to the relative risk
    val number_features: Double = values.size
    val one_plus_e_sum_BX: Double = 1 + Math.exp(pop_log_odds)
    val one_plus_e_sum_Bx: Double = 1 + Math.exp(row_log_odds)
    val one_plus_eBX_over_one_plus_eBx: Double = Math.pow(one_plus_e_sum_BX / one_plus_e_sum_Bx, 1 / number_features)
    // multiply all values with one_plus_eBX_over_one_plus_eBx
    for ((k, _) <- values) values(k) = values(k) * one_plus_eBX_over_one_plus_eBx

    Vectors.sparse(row_log_odds_contribution_vector.size, Helpers.remove_zeros(values).toSeq).asInstanceOf[SparseVector]
  }

  /**
   * For each element x(i) in v1 and y(i) in v2, return e^x(i)/e^y(i) * (pop_log_odds/row_log_odds) to (1/n)
   *
   * @param row_log_odds_contribution_vector row_log_odds_contribution_vector
   * @param population_log_odds_vector       population_log_odds_vector
   * @param row_log_odds                     row_log_odds
   * @param pop_log_odds                     pop_log_odds
   * @return
   */
  def sparse_calculate_relative_contribution_log(row_log_odds_contribution_vector: SparseVector,
                                                 population_log_odds_vector: SparseVector,
                                                 row_log_odds: Double,
                                                 pop_log_odds: Double
                                                ): SparseVector = {
    //     For each element x(i) in v1 and y(i) in v2,
    //     return e^x(i)/e^y(i)  * nth root of 1 + e^(B0 + B1X1 + ... BnXn)/ 1 + e^(B0 + B1x1 + ... Bnxn)
    // :param v1: current row's log odds vector
    //        [ 0.1, 0.2, 0.3 ] means B1x1 = 0.1, B2x2 = 0.2, B2x2 = 0.3
    // :param v2: population log odds vector
    //        [0.1, 0.2, 0.3] means B1X1 = 0.1, B2X2 = 0.2, B3X3 = 0.3

    val values: scala.collection.mutable.Map[Int, Double] =
      scala.collection.mutable.Map[Int, Double]()

    // first calculate eBx / eBX for feature that are set in v1
    for (i <- row_log_odds_contribution_vector.indices.indices) {
      // find the appropriate index on the other side
      val index = row_log_odds_contribution_vector.indices(i)
      val division_factor: Double = Helpers.sparse_vector_get_float_by_index(population_log_odds_vector, index, 0)
      // do the exponent divide
      val eBx: Double = Math.exp(row_log_odds_contribution_vector.values(i))
      val eBX: Double = Math.exp(division_factor)
      val eBx_over_eBX: Double = eBx / eBX
      values(row_log_odds_contribution_vector.indices(i)) = eBx_over_eBX
    }
    // secondly, calculate 1 / eBX for features that are not set in v1
    for (j <- population_log_odds_vector.indices.indices) {
      if (!row_log_odds_contribution_vector.indices.contains(population_log_odds_vector.indices(j))) {
        val eBx: Double = 1
        val eBX: Double = Math.exp(population_log_odds_vector.values(j))
        val eBx_over_eBX = eBx / eBX
        values(population_log_odds_vector.indices(j)) = eBx_over_eBX
      }
    }
    Vectors.sparse(row_log_odds_contribution_vector.size, Helpers.remove_zeros(values).toSeq).asInstanceOf[SparseVector]
  }
}
