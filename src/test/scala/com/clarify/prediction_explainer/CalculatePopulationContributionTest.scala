package com.clarify.prediction_explainer

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{QueryTest, Row, SparkSession}

class CalculatePopulationContributionTest extends QueryTest with SparkSessionTestWrapper {

  val spark2: SparkSession = spark

  import spark2.implicits._

  test("get_related_indices") {
    val ohe_feature_list: Array[String] = Array("foo", "bar")

    println(ohe_feature_list.zipWithIndex.toSeq)
    val result: Seq[Int] = new CalculatePopulationContribution()
      .get_related_indices(ohe_feature_list, "bar")
    assert(result == Array(1).toSeq)
  }
  test("get population log odds for simple feature") {
    val population_log_odds_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val feature_list: Array[String] = Array("foo", "bar")
    val ohe_feature_list: Array[String] = Array("foo", "bar")

    val contribution: Double = new CalculatePopulationContribution()
      .get_population_log_odds_for_feature(population_log_odds_vector, feature_list, ohe_feature_list, 0)
    assert(contribution == 0.1)
  }

  test("get population log odds for non-ohe feature") {
    val population_log_odds_vector = new SparseVector(2, Array(0, 1), Array(0.2, 0.3))
    val feature_list: Array[String] = Array("foo", "bar")
    val ohe_feature_list: Array[String] = Array("foo", "bar")

    val contribution: Double = new CalculatePopulationContribution()
      .get_population_log_odds_for_feature(population_log_odds_vector, feature_list, ohe_feature_list, 0)
    assert(contribution == 0.2)
  }

  test("get population log odds for ohe feature") {
    val population_log_odds_vector = new SparseVector(2, Array(0, 1), Array(0.2, 0.3))
    val feature_list: Array[String] = Array("foo", "bar")
    val ohe_feature_list: Array[String] = Array("foo", "foo")

    val contribution: Double = new CalculatePopulationContribution()
      .get_population_log_odds_for_feature(population_log_odds_vector, feature_list, ohe_feature_list, 0)
    assert(contribution == 0.5)
  }

  test("get population log odds for ohe feature with second feature") {
    val population_log_odds_vector = new SparseVector(2, Array(0, 1), Array(0.2, 0.3))
    val feature_list: Array[String] = Array("foo", "bar")
    val ohe_feature_list: Array[String] = Array("foo", "foo")

    val contribution: Double = new CalculatePopulationContribution()
      .get_population_log_odds_for_feature(population_log_odds_vector, feature_list, ohe_feature_list, 1)
    assert(contribution == 0.5)
  }

  test("calculate pop log odds simple") {
    val ccg_log_odds_vector: SparseVector = new SparseVector(3, Array(0, 1, 2), Array(0.2, 0.3, 0.4))
    val pop_log_odds_vector: SparseVector = new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5))
    val feature_list: Array[String] = Array("foo", "bar", "moo")
    val ohe_feature_list: Array[String] = Array("foo", "foo", "moo")

    val v3 = new CalculatePopulationContribution().sparse_vector_calculate_population_contribution_log_odds(
      ccg_log_odds_vector, pop_log_odds_vector, feature_list, ohe_feature_list)
    assert(v3 == new SparseVector(3, Array(0, 1, 2), Array(0.7, 0.7, 0.5)))
  }

  test("calculate pop log odds simple second feature") {
    val ccg_log_odds_vector: SparseVector = new SparseVector(3, Array(1, 2), Array(0.3, 0.4))
    val pop_log_odds_vector: SparseVector = new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5))
    val feature_list: Array[String] = Array("foo_ohe", "bar_ohe", "moo")
    val ohe_feature_list: Array[String] = Array("foo", "foo", "moo")

    val v3 = new CalculatePopulationContribution().sparse_vector_calculate_population_contribution_log_odds(
      ccg_log_odds_vector, pop_log_odds_vector, feature_list, ohe_feature_list)
    // [0, 0.3, 0.4 ] and [0.3, 0.4, 0.5 ] = [0, 0.3+0.4, 0.5]
    assert(v3 == new SparseVector(3, Array(0, 1, 2), Array(0, 0.3 + 0.4, 0.5)))
  }

  test("calculate pop log odds with zeros") {
    val ccg_log_odds_vector: SparseVector = new SparseVector(3, Array(0, 2), Array(0.2, 0.4))
    val pop_log_odds_vector: SparseVector = new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5))
    val feature_list: Array[String] = Array("foo", "bar", "moo")
    val ohe_feature_list: Array[String] = Array("foo", "foo", "moo")

    val v3 = new CalculatePopulationContribution().sparse_vector_calculate_population_contribution_log_odds(
      ccg_log_odds_vector, pop_log_odds_vector, feature_list, ohe_feature_list)
    assert(v3 == new SparseVector(3, Array(0, 2), Array(0.7, 0.5)))

  }
  test("calculate pop log odds data frame") {
    spark.sharedState.cacheManager.clearCache()
    val data = Seq(
      (
        new SparseVector(3, Array(0, 2), Array(0.2, 0.4)), // row_log_odds_vector
        new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5)), // pop_log_odds_vector
        Seq("male_OHE", "female_OHE", "age"), // feature_list
        Seq("gender", "gender", "age") // ohe_feature_list
      ),
      (
        new SparseVector(3, Array(1, 2), Array(0.2, 0.4)),
        new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5)),
        Seq("male_OHE", "female_OHE", "age"), // feature_list
        Seq("gender", "gender", "age") // ohe_feature_list
      )
    ).toDF()
    val df = data.toDF("row_log_odds_vector", "pop_log_odds_vector", "feature_list", "ohe_feature_list")
    // row_log_odds_vector = [0.2, 0.0, 0.4 ] and pop_log_odds_vector = [0.3, 0.4, 0.5]
    //    = [0.7, 0, 0.5]
    // row_log_odds_vector = [0, 0.3, 0.4] and pop_log_odds_vector = [0.3, 0.4, 0.5]
    //    = [0, 0.7, 0.5]

    df.printSchema()
    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new CalculatePopulationContribution().call _

    spark.udf.register("sparse_vector_calculate_population_contribution_log_odds", add_function)

    val out_df = spark.sql(
      "select sparse_vector_calculate_population_contribution_log_odds(row_log_odds_vector, pop_log_odds_vector, feature_list, ohe_feature_list) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(3, Array(0, 2), Array(0.7, 0.5))),
        Row(new SparseVector(3, Array(1, 2), Array(0.7, 0.5)))
      )
    )
    assert(2 == out_df.count())
  }
}
