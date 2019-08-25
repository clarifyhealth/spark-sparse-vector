package com.clarify.prediction_explainer

import com.clarify.sparse_vectors.{CalculatePopulationContribution, CalculateRelativeContributionLogit, SparkSessionTestWrapper}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{QueryTest, Row}

class CalculateRelativeContributionLogitTest extends QueryTest with SparkSessionTestWrapper {

  val spark2 = spark

  import spark2.implicits._

  test("get population log odds for simple feature") {
    val row_log_odds_contribution_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val population_log_odds_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val row_log_odds = 0.1
    val pop_log_odds = 0.1

    val result: SparseVector = new CalculateRelativeContributionLogit()
      .sparse_calculate_relative_contribution_logit(row_log_odds_contribution_vector,
        population_log_odds_vector,
        row_log_odds,
        pop_log_odds)
    assert(result == new SparseVector(2, Array(0, 1), Array(1.0, 1.0)))
  }

  test("calculate pop log odds data frame") {
    spark.sharedState.cacheManager.clearCache()
    val data = Seq(
      (
        new SparseVector(3, Array(0, 1, 2), Array(0.2, 0.3, 0.4)),
        new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5)),
        Seq((0, "foo", "foo"), (1, "bar", "foo"), (2, "zoo", "zoo"))
      ),
      (
        new SparseVector(3, Array(0, 2), Array(0.2, 0.4)),
        new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5)),
        Seq((0, "foo", "foo"), (1, "bar", "foo"), (2, "zoo", "zoo"))
      )
    ).toDF()
    val df = data.toDF("v1", "v2", "feature_list")
    // df.withColumn("feature_list", df.col("feature_list").cast("array<struct<_1:int,_2:string,_3:string>>"))

    df.printSchema()
    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new CalculatePopulationContribution().call _

    spark.udf.register("sparse_vector_calculate_population_contribution_log_odds", add_function)

    val out_df = spark.sql(
      "select sparse_vector_calculate_population_contribution_log_odds(v1, v2, feature_list) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(3, Array(0, 1, 2), Array(0.7, 0.7, 0.5))),
        Row(new SparseVector(3, Array(0, 2), Array(0.7, 0.5)))
      )
    )
    assert(2 == out_df.count())
  }
}
