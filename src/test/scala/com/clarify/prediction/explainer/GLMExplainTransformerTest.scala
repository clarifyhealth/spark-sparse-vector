package com.clarify.prediction.explainer

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.types.{DataTypes, StructType}

class GLMExplainTransformerTest extends QueryTest with SparkSessionTestWrapper {

  def calculateTotalContrib(
      df: DataFrame,
      featureCoefficients: Map[String, Double]
  ): DataFrame = {
    val encoder =
      RowEncoder.apply(getSchema(df, List("contribSum")))
    df.map(mappingSumRows(df.schema)(featureCoefficients))(
      encoder
    )
  }

  private val mappingSumRows: StructType => Map[String, Double] => Row => Row =
    (schema) =>
      (featureCoefficients) =>
        (row) => {
          val calculate: List[Double] = List(
            featureCoefficients.map {
              case (featureName, _) =>
                row
                  .getDouble(schema.fieldIndex(s"contrib_${featureName}"))
            }.sum + row.getDouble(schema.fieldIndex(s"contrib_intercept"))
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

  test("test powerHalfLink") {

    spark.sharedState.cacheManager.clearCache()

    val predictionDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/predictions.csv").getPath)

    val coefficientsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/coefficients.csv").getPath)

    coefficientsDF.createOrReplaceTempView("my_coefficients")

    // coefficientsDF.show()
    // predictionDF.show()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setLinkFunctionType("powerHalfLink")

    val resultDF = explainTransformer.transform(predictionDF)

    val coefficients = coefficientsDF
      .select("Feature", "Coefficient")
      .filter("not Feature RLIKE '^.*_OHE___unknown$'")
      .collect()

    val allCoefficients = coefficients
      .map(row => (row.getAs[String](0) -> row.getAs[Double](1)))

    val intercept =
      allCoefficients.find(x => x._1 != "Intercept").get._2

    val featureCoefficients =
      allCoefficients.filter(x => x._1 != "Intercept").toMap

    val contribDF =
      calculateTotalContrib(resultDF, featureCoefficients)

    contribDF.select("pred", "contribSum").show()

  }

}
