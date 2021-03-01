package com.clarify.prediction.explainer

import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode}

import scala.collection.immutable.Nil
class EnsembleTreeExplainTransformerTest
    extends QueryTest
    with SharedSparkSession {
  def initialize(): (DataFrame, DataFrame) = {
    val predictionDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/model_ready_test.csv").getPath)

    predictionDF.createOrReplaceTempView("my_predictions")

    val my_schema = StructType(
      StructField("Feature_Index", LongType) ::
        StructField("Feature", StringType) ::
        StructField("Original_Feature", StringType) ::
        StructField("Coefficient", DoubleType) :: Nil
    )

    val coefficientsDF = spark.read
      .option("header", "true")
      .schema(my_schema)
      .csv(getClass.getResource("/basic/feature_importances_test.csv").getPath)

    coefficientsDF.show()
    coefficientsDF.printSchema()

    coefficientsDF.createOrReplaceTempView("my_coefficients")

    (predictionDF, coefficientsDF)
  }

  test("test to run") {

    spark.sharedState.cacheManager.clearCache()

    val (predictionDF, coefficientsDF) = initialize()

    val rf_model_path = getClass.getResource("/test_rf_model").getPath

    val fitted_model= RandomForestRegressionModel.load(rf_model_path)

    val explainTransformer = new EnsembleTreeExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setLabel("label")
    explainTransformer.setModel(fitted_model)
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

    val test_model = RandomForestRegressionModel.load(rf_model_path)
    print(test_model.featureImportances)

    writeToCsv(resultDF)

  }

  def writeToCsv(inputDF: DataFrame): Unit = {

    val features =
      "gender_OHE_Male,gender_OHE_Female,gender_OHE___unknown,gender_OHE_Unknown,race_eth_OHE_White,race_eth_OHE_Black,race_eth_OHE_Asian,race_eth_OHE_Other,race_eth_OHE___unknown,race_eth_OHE_Unknown"
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

    inputDF.select("prediction_label_contrib","ccg_id","contrib_vector").show(false)

    outDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/tmp/rf_out")
  }

}
