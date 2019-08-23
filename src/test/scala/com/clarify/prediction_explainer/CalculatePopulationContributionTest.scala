package com.clarify.prediction_explainer

import com.clarify.sparse_vectors.{Add, CalculatePopulationContribution, SparkSessionTestWrapper}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}

class CalculatePopulationContributionTest extends QueryTest with SparkSessionTestWrapper {

  test("get_population_log_odds_for_feature") {
    val population_log_odds_vector = new SparseVector(3, Array(0, 1), Array(0.1, 0.2))
    val feature_list: Seq[(Int, String, String)] = Seq((0, "foo", "foo"))
    val contribution: Double = new CalculatePopulationContribution()
      .get_population_log_odds_for_feature(population_log_odds_vector, feature_list, 0)
    assert(contribution == 0.1)
  }

  ignore("add simple") {
    val v1 = new SparseVector(3, Array(0), Array(0.1))
    val v2 = new SparseVector(3, Array(0), Array(0.1))
    val feature_list: Seq[(Int, String, String)] = Seq((0, "foo", "foo"))
    val v3 = new CalculatePopulationContribution().sparse_vector_calculate_population_contribution_log_odds(v1, v2, feature_list)
    assert(v3 == new SparseVector(3, Array(0), Array(0.2)))
  }
  ignore("add to other vectors") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(new SparseVector(3, Array(0, 2), Array(0.1, 0.2)), new SparseVector(3, Array(0, 2), Array(0.1, 0.2))),
      Row(new SparseVector(3, Array(0), Array(0.1)), new SparseVector(3, Array(0, 2), Array(0.1, 0.2))),
      Row(new SparseVector(3, Array(0, 1), Array(0.1, 0.1)), new SparseVector(3, Array(0, 2), Array(0.1, 0.5)))
    )

    val fields = List(
      StructField("v1", VectorType, nullable = false),
      StructField("v2", VectorType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new Add().call _

    spark.udf.register("sparse_vector_add", add_function)

    val out_df = spark.sql(
      "select sparse_vector_add(v1, v2) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(3, Array(0, 2), Array(0.2, 0.4))),
        Row(new SparseVector(3, Array(0, 2), Array(0.2, 0.2))),
        Row(new SparseVector(3, Array(0, 1, 2), Array(0.2, 0.1, 0.5)))
      )
    )
    assert(3 == out_df.count())
  }
}
