package com.clarify.prediction_explainer

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{QueryTest, Row, SparkSession}

class CalculateRelativeContributionTest extends QueryTest with SparkSessionTestWrapper {

  val spark2: SparkSession = spark

  import spark2.implicits._

  test("simple logit low probabilities") {
    val row_log_odds_contribution_vector = new SparseVector(3, Array(0, 1), Array(0.1, 0.2))
    val population_log_odds_contribution_vector = new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.6, 0.7))
    val row_log_odds = 0.2
    val pop_log_odds = 0.1

    val result: SparseVector = new CalculateRelativeContribution()
      .sparse_calculate_relative_contribution(
        "logit",
        row_log_odds_contribution_vector,
        population_log_odds_contribution_vector,
        row_log_odds,
        pop_log_odds)

    // row_log_odds_contribution_vector = [0.1, 0.2, 0], population_log_odds_contribution_vector= [0.3, 0.6, 0.7]
    //  = [e^0.1/e^0.3, e^0.2/e^0.6, 1/e^0.7] = [1.1/1.35, 1.22/1.82, 1/2.01]
    // row_log_odds_contribution_vector = [0.1, 0.2, 0], population_log_odds_contribution_vector= [0.3, 0.6, 0.7]
    //  = [e^0.1/e^0.3, e^0.2/e^0.6, 1/e^0.7] = [1.1/1.35, 1.22/1.82, 1/2.01]

    // 1 + e^(pop_log_odds)       1 + e^0.8       3.22
    // --------------------   =   ----------   =  ----- = 1.5
    // 1 + e^(row_log_odds)       1 + e^0.1       2.1
    val z = (1 + Math.exp(pop_log_odds)) / (1 + Math.exp(row_log_odds))
    val number_of_features_root: Double = 1.0 / 3.0
    val z_nth_root = Math.pow(z, number_of_features_root)
    assert(result ==
      new SparseVector(3, Array(0, 1, 2),
        Array(
          (Math.exp(0.1) / Math.exp(0.3)) * z_nth_root,
          (Math.exp(0.2) / Math.exp(0.6)) * z_nth_root,
          (1 / Math.exp(0.7)) * z_nth_root
        )
      )
    )
  }
  test("simple logit high probabilities") {
    val row_log_odds_contribution_vector = new SparseVector(3, Array(0, 1), Array(0.1, 0.2))
    val population_log_odds_contribution_vector = new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.6, 0.7))
    val row_log_odds = 0.1
    val pop_log_odds = 0.8

    val result: SparseVector = new CalculateRelativeContribution()
      .sparse_calculate_relative_contribution(
        "logit",
        row_log_odds_contribution_vector,
        population_log_odds_contribution_vector,
        row_log_odds,
        pop_log_odds)

    // row_log_odds_contribution_vector = [0.1, 0.2, 0], population_log_odds_contribution_vector= [0.3, 0.6, 0.7]
    //  = [e^0.1/e^0.3, e^0.2/e^0.6, 1/e^0.7] = [1.1/1.35, 1.22/1.82, 1/2.01]

    // 1 + e^(pop_log_odds)       1 + e^0.8       3.22
    // --------------------   =   ----------   =  ----- = 1.5
    // 1 + e^(row_log_odds)       1 + e^0.1       2.1
    val z = (1 + Math.exp(pop_log_odds)) / (1 + Math.exp(row_log_odds))
    val number_of_features_root: Double = 1.0 / 3.0
    val z_nth_root = Math.pow(z, number_of_features_root)
    assert(result ==
      new SparseVector(3, Array(0, 1, 2),
        Array(
          (Math.exp(0.1) / Math.exp(0.3)) * z_nth_root,
          (Math.exp(0.2) / Math.exp(0.6)) * z_nth_root,
          (1 / Math.exp(0.7)) * z_nth_root
        )
      )
    )
  }
  test("simple log") {
    val row_log_odds_contribution_vector = new SparseVector(3, Array(0, 1), Array(0.1, 0.2))
    val population_log_odds_contribution_vector = new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.6, 0.7))
    val row_log_odds = 0.1
    val pop_log_odds = 0.8

    val result: SparseVector = new CalculateRelativeContribution()
      .sparse_calculate_relative_contribution(
        "log",
        row_log_odds_contribution_vector,
        population_log_odds_contribution_vector,
        row_log_odds,
        pop_log_odds)

    // row_log_odds_contribution_vector = [0.1, 0.2, 0], population_log_odds_contribution_vector= [0.3, 0.6, 0.7]
    //  = [e^0.1/e^0.3, e^0.2/e^0.6, 1/e^0.7] = [1.1/1.35, 1.22/1.82, 1/2.01]
    assert(result == new SparseVector(3, Array(0, 1, 2),
      Array(Math.exp(0.1) / Math.exp(0.3), Math.exp(0.2) / Math.exp(0.6), 1 / Math.exp(0.7))))
  }
  test("simple identity") {
    val row_log_odds_contribution_vector = new SparseVector(3, Array(0, 1), Array(0.1, 0.2))
    val population_log_odds_contribution_vector = new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.6, 0.7))
    val row_log_odds = 0.1
    val pop_log_odds = 0.8

    val result: SparseVector = new CalculateRelativeContribution()
      .sparse_calculate_relative_contribution(
        "identity",
        row_log_odds_contribution_vector,
        population_log_odds_contribution_vector,
        row_log_odds,
        pop_log_odds)

    // row_log_odds_contribution_vector = [0.1, 0.2, 0], population_log_odds_contribution_vector= [0.3, 0.6, 0.7]
    //  = [e^0.1/e^0.3, e^0.2/e^0.6, 1/e^0.7] = [1.1/1.35, 1.22/1.82, 1/2.01]
    assert(result ==
      new SparseVector(3, Array(0, 1, 2),
        Array(
          0.1 - 0.3,
          0.2 - 0.6,
          0 - 0.7)
      )
    )
  }

  test("calculate pop log odds data frame") {
    spark.sharedState.cacheManager.clearCache()
    val data = Seq(
      (
        new SparseVector(3, Array(0, 1), Array(0.1, 0.2)), // row_log_odds_contribution_vector
        new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.6, 0.7)), // population_log_odds_vector
        0.1, // row_log_odds
        0.2 // pop_log_odds
      ),
      (
        new SparseVector(3, Array(0, 1), Array(0.3, 0.4)),
        new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.6, 0.7)),
        0.1,
        0.2
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

    val z = (1 + Math.exp(0.2)) / (1 + Math.exp(0.1))
    val number_of_features_root: Double = 1.0 / 3.0
    val z_nth_root = Math.pow(z, number_of_features_root)

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(
          new SparseVector(3, Array(0, 1, 2),
            Array(
              (Math.exp(0.1) / Math.exp(0.3)) * z_nth_root,
              (Math.exp(0.2) / Math.exp(0.6)) * z_nth_root,
              (1 / Math.exp(0.7)) * z_nth_root
            )
          )
        ),
        Row(
          new SparseVector(3, Array(0, 1, 2),
            Array(
              (Math.exp(0.3) / Math.exp(0.3)) * z_nth_root,
              (Math.exp(0.4) / Math.exp(0.6)) * z_nth_root,
              (1 / Math.exp(0.7)) * z_nth_root
            )
          )
        )
      )
    )
    assert(2 == out_df.count())
  }
}
