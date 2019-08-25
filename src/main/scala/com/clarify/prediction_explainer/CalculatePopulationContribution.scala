package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.api.java.UDF4

case class FeatureListItem(feature_index: Int, feature_name: String, base_feature_name: String) extends Serializable

class CalculatePopulationContribution
  extends UDF4[
    SparseVector,
    SparseVector,
    Seq[String],
    Seq[String],
    SparseVector
  ] {

  override def call(
                     v1: SparseVector,
                     v2: SparseVector,
                     feature_list: Seq[String],
                     ohe_feature_list: Seq[String]
                   ): SparseVector = {
    sparse_vector_calculate_population_contribution_log_odds(v1, v2, feature_list, ohe_feature_list)
  }

  def sparse_vector_calculate_population_contribution_log_odds(
                                                                ccg_log_odds_vector: SparseVector,
                                                                pop_log_odds_vector: SparseVector,
                                                                feature_list: Seq[String],
                                                                ohe_feature_list: Seq[String]
                                                              ): SparseVector = {
    // Calculates the population log odds for a ccg
    // if x1 and x2 are one hot encoded values of the same feature
    //     uses the formula B1X1 + B2X2
    // else
    //     uses the formula B1X1
    // :param v1: current row's feature contribution vector
    //        [ 0.1, 0.2, 0.3 ] means B1x1 = 0.1, B2x2 = 0.2, B2x2 = 0.3
    // :param v2: population feature contribution vector
    //        [0.1, 0.2, 0.3] means B1X1 = 0.1, B2X2 = 0.2, B3X3 = 0.3
    // :param feature_list: list of feature indices (feature_index, feature_name, ohe_feature_name)

    require(pop_log_odds_vector.size == feature_list.size,
      "pop_log_odds_vector is not the same size as feature_list")
    require(ccg_log_odds_vector.size <= pop_log_odds_vector.size,
      "ccg_log_odds_vector is longer than pop_log_odds_vector")

    val values: scala.collection.mutable.Map[Int, Double] =
      scala.collection.mutable.Map[Int, Double]()

    // first calculate contribution for features in v1
    for (i <- 0 until (ccg_log_odds_vector.indices.size)) {
      // find the appropriate index on the other side
      val index = ccg_log_odds_vector.indices(i)

      val population_log_odds: Double = get_population_log_odds_for_feature(
        pop_log_odds_vector,
        feature_list,
        ohe_feature_list,
        index)
      values(ccg_log_odds_vector.indices(i)) = population_log_odds
    }
    // now add population contribution for features that are not in the ccg vector
    //    except for the OHE features since they are summed up above
    for (j <- 0 until (pop_log_odds_vector.indices.size)) {
      val index = pop_log_odds_vector.indices(j)
      if (ccg_log_odds_vector.indices.contains(index) == false) {
        // feature is not already in the ccg vector
        val feature_name = feature_list(index)
        val ohe_feature_name = ohe_feature_list(index)
        if (feature_name == ohe_feature_name) { // not an OHE
          values(pop_log_odds_vector.indices(j)) = pop_log_odds_vector.values(j)
        }
      }
    }

    Vectors.sparse(ccg_log_odds_vector.size, Helpers.remove_zeros(values).toSeq).asInstanceOf[SparseVector]
  }

  def get_population_log_odds_for_feature(pop_log_odds_vector: SparseVector,
                                          feature_list: Seq[String],
                                          ohe_feature_list: Seq[String],
                                          index: Int): Double = {
    require(pop_log_odds_vector.size == feature_list.size,
      "pop_log_odds_vector is not the same size as feature_list")

    // find the corresponding entry in feature_list for this feature
    val ohe_feature_name: String = ohe_feature_list(index)
    // get OHE (one hot encoded) feature name. In case of OHE this is the feature name for all OHE values.
    //    otherwise it is the same as the feature name
    // find all features related to this ohe feature
    val related_feature_indices: Seq[Int] =
    get_related_indices(ohe_feature_list, ohe_feature_name)

    // calculate the population log odds by adding Bjxj of all the related features
    var population_log_odds: Double = 0
    for (j <- 0 until (pop_log_odds_vector.indices.size)) {
      if (related_feature_indices.contains(pop_log_odds_vector.indices(j))) {
        population_log_odds = population_log_odds + pop_log_odds_vector.values(j)
      }
    }
    population_log_odds
  }


  def get_related_indices(ohe_feature_list: Seq[String], ohe_feature_name: String): Seq[Int] = {
    ohe_feature_list.zipWithIndex
      .filter(x => x._1 == ohe_feature_name).map(x => x._2)
  }
}
