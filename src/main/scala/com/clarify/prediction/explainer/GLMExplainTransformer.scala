package com.clarify.prediction.explainer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

class GLMExplainTransformer(override val uid: String) extends Transformer {

  // Transformer Params
  // Defining a Param requires 3 elements:
  //  - Param definition
  //  - Param getter method
  //  - Param setter method
  // (The getter and setter are technically not required, but they are nice standards to follow.)

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
    s"(1 / (1 + np.exp(-${x}))"
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

  private val sum: List[String] => String = { x: List[String] =>
    x.mkString("+")
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
      .map(row => (row.getAs[String](0) -> row.getAs[Float](1)))

    val intercept =
      allCoefficients.find(x => x._1 != "Intercept").get._2

    val featureCoefficients =
      allCoefficients.filter(x => x._1 != "Intercept").toMap

    val linearContrib = featureCoefficients.map {
      case (featureName, coefficient) => s"(${featureName} * ${coefficient})"
    }.toList

    val linearContribPositive =
      featureCoefficients.map {
        case (featureName, coefficient) =>
          s"(case when ${featureName} * ${coefficient} < 0 then 0 else ${featureName} * ${coefficient} end )"
      }.toList

    val linearContribNegative =
      featureCoefficients.map {
        case (featureName, coefficient) =>
          s"(case when ${featureName} * ${coefficient} > 0 then 0 else ${featureName} * ${coefficient} end )"
      }.toList

    val sigmaDF = dataset.withColumn("sigma", expr(sum(linearContrib)))

    val sigmaPosDF =
      sigmaDF.withColumn("sigmaPos", expr(sum(linearContribPositive)))

    val sigmaNegDF =
      sigmaPosDF.withColumn("sigmaNeg", expr(sum(linearContribNegative)))

    val predDf = sigmaNegDF.withColumn(
      "pred",
      expr(linkFunction(s"sigma + $intercept"))
    )

    val predPosDF = predDf.withColumn(
      "predPos",
      expr(linkFunction(s"sigmaPos + $intercept"))
    )

    val predNegDF = predPosDF.withColumn(
      "predNeg",
      expr(linkFunction(s"sigmaNeg + $intercept"))
    )

    val contribInterceptDF = predNegDF.withColumn(
      "contrib_intercept",
      expr(linkFunction(s"$intercept"))
    )

    val deficitDF = contribInterceptDF.withColumn(
      "deficit",
      expr("(pred + contrib_intercept) - (predPos + predNeg)")
    )

    val contribPosDF = deficitDF.withColumn(
      "contribPos",
      expr("(predPos - contrib_intercept + deficit) / 2")
    )
    val contribNegsDF = contribPosDF.withColumn(
      "contribNeg",
      expr("(predNeg  - contrib_intercept + deficit) / 2")
    )

    val contributions = featureCoefficients.map {
      case (featureName, coefficient) =>
        s"""((case when ${featureName} * ${coefficient} < 0 then 0 else ${featureName} * ${coefficient} end) * contribPos 
           | / 
           | (case when sigmaPos = 0 then 1 else sigmaPos end))
           | / 
           |((case when ${featureName} * ${coefficient} > 0 then 0 else ${featureName} * ${coefficient} end) * contribNeg 
           |/ (case when sigmaNeg = 0 then 1 else sigmaNeg end)) as contrib_${featureName}""".stripMargin
    }.toList

    val contribDF = contribNegsDF.select("*", contributions: _*)

    contribDF
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
