package com.clarify.model.meta

import org.apache.log4j.Logger
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.{
  DefaultParamsReadable,
  DefaultParamsWritable,
  Identifiable
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{avg, lit, substring_index}
import org.json4s._
import org.json4s.jackson.Json

class ModelMetaTransformer(override val uid: String)
    extends Transformer
    with DefaultParamsWritable {

  val logger: Logger = Logger.getLogger(getClass)

  def this() = this(Identifiable.randomUID("ModelMetaTransformer"))

  // Transformer Params
  // Defining a Param requires 3 elements:
  //  - Param definition
  //  - Param getter method
  //  - Param setter method
  // (The getter and setter are technically not required, but they are nice standards to follow.)

  /**
    * Param for predictionView view name.
    */
  final val predictionView: Param[String] =
    new Param[String](
      this,
      "predictionView",
      "input predictionView view name"
    )

  final def getPredictionView: String = $(predictionView)

  final def setPredictionView(value: String): ModelMetaTransformer =
    set(predictionView, value)

  /**
    * Param for coefficientView name.
    */
  final val coefficientView: Param[String] =
    new Param[String](this, "coefficientView", "input coefficient view name")

  final def getCoefficientView: String = $(coefficientView)

  final def setCoefficientView(value: String): ModelMetaTransformer =
    set(coefficientView, value)

  /**
    * Param for modelMetaView name.
    */
  final val modelMetaView: Param[String] =
    new Param[String](this, "modelMetaView", "input model meta view name")

  final def getModelMetaView: String = $(modelMetaView)

  final def setModelMetaView(value: String): ModelMetaTransformer =
    set(modelMetaView, value)

  /**
    * Param for link function type .
    */
  final val linkFunction: Param[String] =
    new Param[String](this, "linkFunction", "input linkFunction")

  final def getLinkFunction: String = $(linkFunction)

  final def setLinkFunction(value: String): ModelMetaTransformer =
    set(linkFunction, value)

  /**
    * Param for label name.
    */
  final val labelCol: Param[String] =
    new Param[String](
      this,
      "labelCol",
      "training label name"
    )

  final def getLabelCol: String = $(labelCol)

  final def setLabelCol(value: String): ModelMetaTransformer =
    set(labelCol, value)

  /**
    * Param for features name.
    */
  final val featuresCol: Param[String] =
    new Param[String](
      this,
      "featuresCol",
      "training features col name"
    )

  final def getFeaturesCol: String = $(featuresCol)

  final def setFeaturesCol(value: String): ModelMetaTransformer =
    set(featuresCol, value)

  /**
    * Param for family name.
    */
  final val family: Param[String] =
    new Param[String](
      this,
      "family",
      "glm family name"
    )

  final def getFamily: String = $(family)

  final def setFamily(value: String): ModelMetaTransformer =
    set(family, value)

  /**
    * Param for variancePower.
    */
  final val variancePower: Param[Double] =
    new Param[Double](
      this,
      "variancePower",
      "tweedie variancePower"
    )

  final def getVariancePower: Double = $(variancePower)

  final def setVariancePower(value: Double): ModelMetaTransformer =
    set(variancePower, value)

  /**
    * Param for linkPower.
    */
  final val linkPower: Param[Double] =
    new Param[Double](
      this,
      "linkPower",
      "tweedie linkPower"
    )

  final def getLinkPower: Double = $(linkPower)

  final def setLinkPower(value: Double): ModelMetaTransformer =
    set(linkPower, value)

  // (Optional) You can set defaults for Param values if you like.
  setDefault(
    predictionView -> "predictions",
    coefficientView -> "coefficient",
    modelMetaView -> "modelMeta",
    linkFunction -> "other",
    labelCol -> "test",
    featuresCol -> "features",
    family -> "gaussian",
    linkPower -> 0.0,
    variancePower -> -1.0
  )

  // Transformer requires 3 methods:
  //  - transform
  //  - transformSchema
  //  - copy

  /**
    * This method implements the main transformation.
    * Its required semantics are fully defined by the method API: take a Dataset or DataFrame,
    * and return a DataFrame.
    *
    * Most Transformers are 1-to-1 row mappings which add one or more new columns and do not
    * remove any columns.  However, this restriction is not required.  This example does a flatMap,
    * so we could either (a) drop other columns or (b) keep other columns, making copies of values
    * in each row as it expands to multiple rows in the flatMap.  We do (a) for simplicity.
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    import dataset.sqlContext.implicits._

    logger.info(
      s"""All parameters 
         | predictionView=${getPredictionView} ,
         | coefficientView=${getCoefficientView} ,
         | modelMetaView=${getModelMetaView} , 
         | linkFunction=${getLinkFunction} , 
         | labelCol=${getLabelCol} ,
         | featuresCol=${getFeaturesCol} ,
         | family=${getFamily} ,
         | linkPower=${getLinkPower} ,
         | variancePower= ${getVariancePower}""".stripMargin
    )

    logger.info(s"Loading CoefficientView ${getCoefficientView}")
    // Load coefficientView
    val coefficientsDF = dataset.sqlContext
      .table(getCoefficientView)

    logger.info(s"Loading PredictionView ${getPredictionView}")
    // Load predictionView
    val predictionsDF = dataset.sqlContext.table(getPredictionView)

    logger.info(s"Loading cms_hcc_descriptions")
    // Load cms_hcc_descriptions
    val hccDescriptionsDF = dataset.sqlContext.table("cms_hcc_descriptions")

    val coefficients = coefficientsDF
      .select("Feature_Index", "Feature", "Coefficient")
      .withColumn("ohe_features", substring_index($"Feature", "_OHE", 1))
      .orderBy("Feature_Index")
      .collect()

    logger.info(
      s"Start ${getPredictionView} population feature and contrib means calculation"
    )
    // The most expensive operation
    val population_means = predictionsDF
      .select(
        Summarizer.mean($"${getFeaturesCol}").alias("pop_mean"),
        Summarizer.mean($"contrib_vector").alias("pop_contribution"),
        avg(s"prediction_${getLabelCol}_sigma").alias("sigma_mean")
      )
      .collect()(0)
    logger.info(
      s"Done ${getPredictionView} population feature and contrib means calculation"
    )

    // Calculate the population feature and contrib means
    val pop_mean = population_means.getAs[Vector]("pop_mean").toArray
    val pop_contribution =
      population_means.getAs[Vector]("pop_contribution").toArray
    val sigma_mean = population_means.getAs[Double]("sigma_mean")

    val coefficientsMap = coefficients
      .map(
        row =>
          (row.getAs[String]("Feature") -> row.getAs[Double]("Coefficient"))
      )
      .toMap

    // Intercept
    val intercept = coefficientsMap.getOrElse("Intercept", 0.0)

    // Feature and Coefficient
    val featureCoefficients = coefficientsMap.filter {
      case (feature, _) => feature != "Intercept"
    }

    // OHE Feature
    val oheFeatures = coefficients
      .map(
        row => (row.getAs[String]("ohe_features"))
      )
      .filter(x => x != "Intercept")

    // hcc descriptions handling
    val hccDescriptions = hccDescriptionsDF
      .collect()
      .map(
        row =>
          (row.getAs[String]("hcc_code") -> row.getAs[String]("description"))
      )
      .toMap

    val hccDescriptionsMap = featureCoefficients.map {
      case (feature, coefficient) =>
        s"${feature} (${hccDescriptions.getOrElse(feature, "")})" -> coefficient
    }

    val hccDescriptionsJson = Json(DefaultFormats).write(hccDescriptionsMap)

    logger.info(
      s"Done ${getPredictionView} HCC Description mapping ${hccDescriptionsJson}"
    )

    logger.info(
      s"Start ${getPredictionView}  Coefficient Summary"
    )
    val summaryRow = coefficientsDF.select($"model_id").limit(1)

    val summaryRowCoefficientsDF = summaryRow
      .withColumn("pop_mean", lit(pop_mean))
      .withColumn("pop_contribution", lit(pop_contribution))
      .withColumn("sigma_mean", lit(sigma_mean))
      .withColumn("link_function", lit(getLinkFunction))
      .withColumn("family", lit(getFamily))
      .withColumn("link_power", lit(getLinkPower))
      .withColumn("variance_power", lit(getLinkPower))
      .withColumn("intercept", lit(intercept))
      .withColumn("coefficients", lit(featureCoefficients.values.toArray))
      .withColumn("features", lit(featureCoefficients.keys.toArray))
      .withColumn("ohe_features", lit(oheFeatures))
      .withColumn("feature_coefficients", lit(hccDescriptionsJson))

    logger.info(
      s"Done ${getPredictionView} Coefficient Summary"
    )

    logger.info(
      s"Start ${getPredictionView} Prediction Summary"
    )
    val predictionsOneRowDF = predictionsDF.limit(1)

    val regressionMetric = fetchRegressionMetric(predictionsOneRowDF)
    val classificationMetric = fetchClassificationMetric(predictionsOneRowDF)

    val projections = Seq("*") ++ regressionMetric.map {
      case (key, value) => s"cast(${value} as double) as ${key}"
    } ++ classificationMetric.map {
      case (key, value) => s"cast(${value} as double) as ${key}"
    }

    val finalDF = summaryRowCoefficientsDF.selectExpr(projections: _*)

    finalDF.show(truncate = false)

    finalDF.createOrReplaceTempView(getModelMetaView)

    logger.info(
      s"Done ${getPredictionView} Prediction Summary"
    )

    finalDF
  }

  /**
    * Extract the regression metric from a single row prediction DataFrame
    * @param prediction This is one Row DataFrame
    * @return
    */
  def fetchRegressionMetric(prediction: DataFrame): Map[String, AnyVal] = {
    if (prediction.columns.contains("bias_avg")) {
      val oneRow = prediction
        .selectExpr(
          "r2",
          s"prediction_${getLabelCol}_rmse as rmse",
          "mae",
          "bias_avg",
          "zero_residuals",
          "count_total"
        )
        .collect()(0)
      oneRow.getValuesMap[AnyVal](oneRow.schema.fieldNames)
    } else {
      Map(
        "r2" -> -1.0,
        "rmse" -> -1.0,
        "mae" -> -1.0,
        "bias_avg" -> -1.0,
        "zero_residuals" -> -1.0,
        "count_total" -> -1.0
      )
    }
  }

  /**
    * Extract the classification metric from a single row prediction DataFrame
    * @param prediction This is one Row DataFrame
    * @return
    */
  def fetchClassificationMetric(prediction: DataFrame): Map[String, AnyVal] = {
    if (prediction.columns.contains("accuracy")) {
      val oneRow = prediction
        .select(
          "accuracy",
          "f1",
          "aucROC",
          "aucPR",
          "weightedPrecision",
          "weightedRecall"
        )
        .collect()(0)
      oneRow.getValuesMap[AnyVal](oneRow.schema.fieldNames)
    } else {
      Map(
        "accuracy" -> -1.0,
        "f1" -> -1.0,
        "aucROC" -> -1.0,
        "aucPR" -> -1.0,
        "weightedPrecision" -> -1.0,
        "weightedRecall" -> -1.0
      )
    }
  }

  /**
    * Check transform validity and derive the output schema from the input schema.
    *
    * We check validity for interactions between parameters during `transformSchema` and
    * raise an exception if any parameter value is invalid. Parameter value checks which
    * do not depend on other parameters are handled by `Param.validate()`.
    *
    * Typical implementation should first conduct verification on schema change and parameter
    * validity, including complex parameter interaction checks.
    */
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  /**
    * Creates a copy of this instance.
    * Requirements:
    *  - The copy must have the same UID.
    *  - The copy must have the same Params, with some possibly overwritten by the `extra`
    *    argument.
    *  - This should do a deep copy of any data members which are mutable.  That said,
    *    Transformers should generally be immutable (except for Params), so the `defaultCopy`
    *    method often suffices.
    * @param extra  Param values which will overwrite Params in the copy.
    */
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object ModelMetaTransformer
    extends DefaultParamsReadable[ModelMetaTransformer] {
  override def load(path: String): ModelMetaTransformer = super.load(path)
}
