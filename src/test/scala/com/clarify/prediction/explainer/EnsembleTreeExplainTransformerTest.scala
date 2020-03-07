package com.clarify.prediction.explainer

import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode}
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

    val rf_model_path = getClass.getResource("/test_rf_model").getPath

    val explainTransformer = new EnsembleTreeExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setLabel("label")
    explainTransformer.setModelPath(rf_model_path)
    explainTransformer.setDropPathColumn(false)

    val df = spark.emptyDataFrame
    val resultDF = explainTransformer.transform(df)

    val outDF = resultDF
      .selectExpr(
        "*",
        "bround(glm_contrib_intercept+glm_contribs_sum,3) as glm_predict",
        "bround(prediction_label_contrib_intercept+prediction_label_contrib_sum,3) as rf_prediction",
        "size(paths) as path_size"
      )

    assert(predictionDF.count() == outDF.count())

    outDF.show()

    val model = RandomForestRegressionModel.load(rf_model_path)
    print(model.featureImportances)

    writeToCsv(resultDF)

  }

  def writeToCsv(inputDF: DataFrame): Unit = {

    val features =
      "sex_male,sex_female,age_0,age_1,age_2,age_3,age_4,age_5,age_6,age_7,age_8,age_9,age_10,age_11"
        .split(",")

    val rfContrib = (0 until features.length)
      .map(
        i => s"prediction_label_contrib[${i}] as contrib_${features(i)}_rf"
      )

    val glmContrib = (0 until features.length)
      .map(
        i => s"contrib_${features(i)}"
      )

    val contributions = Seq("ccg_id") ++ glmContrib ++ rfContrib ++ Seq(
      "glm_contrib_intercept as contrib_intercept",
      "prediction_label_contrib_intercept as contrib_intercept_rf"
    )

    val outDF = inputDF.selectExpr(contributions: _*)

    outDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/tmp/rf_out")
  }

}
