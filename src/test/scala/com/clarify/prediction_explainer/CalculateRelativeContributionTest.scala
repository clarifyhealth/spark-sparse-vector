package com.clarify.prediction_explainer

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{QueryTest, Row, SparkSession}

class CalculateRelativeContributionTest extends QueryTest with SparkSessionTestWrapper {

  val spark2: SparkSession = spark

  import spark2.implicits._

  test("simple logit") {
    val row_log_odds_contribution_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val population_log_odds_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val row_log_odds = 0.1
    val pop_log_odds = 0.1

    val result: SparseVector = new CalculateRelativeContribution()
      .sparse_calculate_relative_contribution(
        "logit",
        row_log_odds_contribution_vector,
        population_log_odds_vector,
        row_log_odds,
        pop_log_odds)
    assert(result == new SparseVector(2, Array(0, 1), Array(1.0, 1.0)))
  }

  test("simple log") {
    val row_log_odds_contribution_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val population_log_odds_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val row_log_odds = 0.1
    val pop_log_odds = 0.1

    val result: SparseVector = new CalculateRelativeContribution()
      .sparse_calculate_relative_contribution(
        "log",
        row_log_odds_contribution_vector,
        population_log_odds_vector,
        row_log_odds,
        pop_log_odds)
    assert(result == new SparseVector(2, Array(0, 1), Array(1.0, 1.0)))
  }

  test("calculate pop log odds data frame") {
    spark.sharedState.cacheManager.clearCache()
    val data = Seq(
      (
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)),
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)),
        0.1,
        0.1
      ),
      (
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)),
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)),
        0.1,
        0.1
      )
    ).toDF()
    val df = data.toDF("row_log_odds_contribution_vector",
      "population_log_odds_vector",
      "row_log_odds",
      "pop_log_odds")

    df.printSchema()
    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new CalculateRelativeContribution().call _

    spark.udf.register("sparse_calculate_relative_contribution", add_function)

    val out_df = spark.sql(
      "select sparse_calculate_relative_contribution('logit', row_log_odds_contribution_vector, population_log_odds_vector, row_log_odds, pop_log_odds) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(2, Array(0, 1), Array(1.0, 1.0))),
        Row(new SparseVector(2, Array(0, 1), Array(1.0, 1.0)))
      )
    )
    assert(2 == out_df.count())
  }
}
