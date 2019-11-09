package com.clarify.prediction.explainer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class GLMExplainTransformer(override val uid: String) extends Transformer {

  // Transformer Params
  // Defining a Param requires 3 elements:
  //  - Param definition
  //  - Param getter method
  //  - Param setter method
  // (The getter and setter are technically not required, but they are nice standards to follow.)
  def this() = this(Identifiable.randomUID("GLMExplainTransformer"))

  /**
    * Param for input column name.
    */
  final val coefficientView: Param[String] =
    new Param[String](this, "coefficientView", "input coefficient view name")

  final def getCoefficientView: String = $(coefficientView)

  final def setCoefficientView(value: String): GLMExplainTransformer =
    set(coefficientView, value)

  final val linkFunctionType: Param[String] =
    new Param[String](this, "linkFunctionType", "input linkFunction name")

  final def getLinkFunctionType: String = $(linkFunctionType)

  final def setLinkFunctionType(value: String): GLMExplainTransformer =
    set(linkFunctionType, value)

  // (Optional) You can set defaults for Param values if you like.
  setDefault(
    coefficientView -> "coefficient",
    linkFunctionType -> "powerHalfLink"
  )

  private val logLink: String => String = { x: String =>
    s"exp(${x})"
  }
  private val expLink: String => String = { x: String =>
    s"log(${x})"
  }
  private val logitLink: String => String = { x: String =>
    s"1 / (1 + exp(-${x}))"
  }
  private val powerHalfLink: String => String = { x: String =>
    s"pow(${x},2)"
  }
  private val identityLink: String => String = { x: String =>
    s"${x}"
  }

  def buildLinkFunction(linkFunctionType: String): String => String =
    (x: String) => {
      linkFunctionType match {
        case "logLink"      => logLink(x)
        case "expLink"      => expLink(x)
        case "logitLink"    => logitLink(x)
        case "identityLink" => identityLink(x)
        case _              => powerHalfLink(x)
      }
    }

  private val keepPositive: Double => Double = (temp: Double) => {
    if (temp < 0.0) 0.0 else temp
  }
  private val keepNegative: Double => Double = (temp: Double) => {
    if (temp > 0.0) 0.0 else temp
  }
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

    val linkFunction = buildLinkFunction($(linkFunctionType))

    val coefficients = dataset.sqlContext
      .table($(coefficientView))
      .select("Feature", "Coefficient")
      .filter("not Feature RLIKE '^.*_OHE___unknown$'")
      .collect()

    val allCoefficients = coefficients
      .map(row => (row.getAs[String](0) -> row.getAs[Double](1)))

    val intercept =
      allCoefficients.find(x => x._1 == "Intercept").get._2

    val featureCoefficients =
      allCoefficients.filter(x => x._1 != "Intercept").toMap

    val df = calculateLinearContributions(
      dataset.toDF(),
      featureCoefficients,
      "linear_contrib"
    )
    val dfWithSigma = calculateSigma(df, featureCoefficients)

    val predDf = dfWithSigma.withColumn(
      "calculated_prediction",
      expr(linkFunction(s"sigma + $intercept"))
    )

    val predPosDF = predDf.withColumn(
      "prediction_positive",
      expr(linkFunction(s"sigma_positive + $intercept"))
    )

    val predNegDF = predPosDF.withColumn(
      "prediction_negative",
      expr(linkFunction(s"sigma_negative + $intercept"))
    )

    val contribInterceptDF = predNegDF.withColumn(
      "contrib_intercept",
      expr(linkFunction(s"$intercept"))
    )

    val deficitDF = contribInterceptDF.withColumn(
      "deficit",
      expr(
        "calculated_prediction + contrib_intercept - (prediction_positive + prediction_negative)"
      )
    )

    val contribPosDF = deficitDF.withColumn(
      "contrib_positive",
      expr("prediction_positive - contrib_intercept + deficit / 2")
    )
    val contribNegsDF = contribPosDF.withColumn(
      "contrib_negative",
      expr("prediction_negative - contrib_intercept + deficit / 2")
    )

    val contributionsDF =
      calculateContributions(contribNegsDF, featureCoefficients, "contrib")
    contributionsDF.show()

    contributionsDF
  }

  private def calculateLinearContributions(
      df: DataFrame,
      featureCoefficients: Map[String, Double],
      prefix: String
  ): DataFrame = {
    val encoder =
      buildEncoder(df, featureCoefficients, prefix)
    df.map(mappingLinearContributionsRows(df.schema)(featureCoefficients))(
      encoder
    )
  }

  private val mappingLinearContributionsRows
      : StructType => Map[String, Double] => Row => Row =
    (schema) =>
      (featureCoefficients) =>
        (row) => {
          val calculate: List[Double] = featureCoefficients.map {
            case (featureName, coefficient) =>
              row
                .get(schema.fieldIndex(featureName))
                .toString
                .toDouble * coefficient
          }.toList
          Row.merge(row, Row.fromSeq(calculate))
        }

  private def calculateSigma(
      df: DataFrame,
      featureCoefficients: Map[String, Double]
  ): DataFrame = {
    val encoder =
      RowEncoder.apply(
        getSchema(df, List("sigma", "sigma_positive", "sigma_negative"))
      )
    df.map(mappingSigmaRows(df.schema)(featureCoefficients))(encoder)
  }

  private val mappingSigmaRows
      : StructType => Map[String, Double] => Row => Row =
    (schema) =>
      (featureCoefficients) =>
        (row) => {
          val calculate: List[Double] = List(
            featureCoefficients.map {
              case (featureName, _) =>
                row
                  .getDouble(
                    schema.fieldIndex(s"linear_contrib_${featureName}")
                  )
            }.sum,
            featureCoefficients.map {
              case (featureName, _) =>
                val temp =
                  row.getDouble(
                    schema.fieldIndex(s"linear_contrib_${featureName}")
                  )
                keepPositive(temp)
            }.sum,
            featureCoefficients.map {
              case (featureName, _) =>
                val temp =
                  row.getDouble(
                    schema.fieldIndex(s"linear_contrib_${featureName}")
                  )
                keepNegative(temp)
            }.sum
          )
          Row.merge(row, Row.fromSeq(calculate))
        }

  private def getSchema(
      df: DataFrame,
      columnNames: List[String]
  ): StructType = {
    var schema: StructType = df.schema
    columnNames.foreach {
      case (featureName) =>
        schema = schema.add(s"${featureName}", DataTypes.DoubleType, false)
    }
    schema
  }

  private def calculateContributions(
      df: DataFrame,
      featureCoefficients: Map[String, Double],
      prefix: String
  ): DataFrame = {
    val encoder =
      buildEncoder(df, featureCoefficients, prefix)
    df.map(
      mappingContributionsRows(df.schema)(featureCoefficients)
    )(encoder)
  }

  private def buildEncoder(
      df: DataFrame,
      featureCoefficients: Map[String, Double],
      prefix: String
  ) = {
    RowEncoder.apply(
      getSchema(
        df,
        featureCoefficients.keys.map(x => s"${prefix}_${x}").toList
      )
    )
  }

  private val mappingContributionsRows
      : StructType => Map[String, Double] => Row => Row =
    (schema) =>
      (featureCoefficients) =>
        (row) => {
          val calculate: List[Double] = featureCoefficients.map {
            case (featureName, _) =>
              val temp = row.getDouble(
                schema.fieldIndex(s"linear_contrib_${featureName}")
              )
              val sigmaPos = row
                .getDouble(
                  schema.fieldIndex("sigma_positive")
                )
              val sigmaPosZeroReplace = if (sigmaPos == 0.0) 1.0 else sigmaPos

              val sigmaNeg = row
                .getDouble(
                  schema.fieldIndex("sigma_negative")
                )
              val sigmaNegZeroReplace = if (sigmaNeg == 0.0) 1.0 else sigmaNeg

              val contribPos =
                row.getDouble(schema.fieldIndex("contrib_positive"))
              val contribNeg =
                row.getDouble(schema.fieldIndex("contrib_negative"))

              keepPositive(temp) * contribPos / sigmaPosZeroReplace +
                keepNegative(temp) * contribNeg / sigmaNegZeroReplace
          }.toList
          Row.merge(row, Row.fromSeq(calculate))
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
