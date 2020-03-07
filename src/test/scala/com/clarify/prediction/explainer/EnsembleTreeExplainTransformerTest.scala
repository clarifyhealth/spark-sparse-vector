package com.clarify.prediction.explainer

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, QueryTest}

class EnsembleTreeExplainTransformerTest
    extends QueryTest
    with SharedSparkSession {
  def initialize(): (DataFrame, DataFrame) = {
    val predictionDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/rf_prediction_test.csv").getPath)

    predictionDF.createOrReplaceTempView("my_predictions")

    val coefficientsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/feature_importances.csv").getPath)

    coefficientsDF.createOrReplaceTempView("my_coefficients")

    (predictionDF, coefficientsDF)
  }

  test("test to run") {

    spark.sharedState.cacheManager.clearCache()

    val (predictionDF, coefficientsDF) = initialize()

    val explainTransformer = new EnsembleTreeExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setLabel("label")
    explainTransformer.setModelPath(
      getClass.getResource("/test_rf_model").getPath
    )

    val df = spark.emptyDataFrame
    val resultDF = explainTransformer.transform(df)

    val outDF = resultDF
      .selectExpr(
        "*",
        "bround(glm_contrib_intercept+glm_contribs_sum,3) as glm_predict",
        "bround(prediction_label_contrib_intercept+prediction_label_contrib_sum,3) as rf_prediction"
      )

    outDF.show()

  }
}
