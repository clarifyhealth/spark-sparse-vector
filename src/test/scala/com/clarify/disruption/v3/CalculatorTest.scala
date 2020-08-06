package com.clarify.disruption.v3

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types.DataTypes

class CalculatorTest extends QueryTest with SparkSessionTestWrapper {
  test("direct test max at beginning") {
    val calculator = new Calculator()
    val data: Seq[Double] = Seq(2.0, 1.6666666666666665, 1.3333333333333333, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    val result: Option[Double] = calculator.call(data)
    assert(result.isDefined)
    assert(result.get == -0.3333333333333335)
  }
  test("direct test max in the middle") {
    val calculator = new Calculator()
    val data: Seq[Double] = Seq(2.0, 2,0, 2,0, 2.0, 1.6666666666666665, 1.3333333333333333, 1.0, 1.0, 1.0, 1.0)
    val result: Option[Double] = calculator.call(data)
    assert(result.isDefined)
    assert(result.get == -2.0)
  }
  test("basic test") {

    val testDF = spark.sql(
      s"select cast(array(2.0, 2,0, 2,0, 2.0, 1.6666666666666665, 1.3333333333333333, 1.0, 1.0, 1.0, 1.0) as array<double>) as data"
    )

    spark.udf.register(
      "calculate_disruption",
      new Calculator(),
      DataTypes.DoubleType
    )

    val resultDF = testDF.selectExpr(
      "data",
      "calculate_disruption(data) as calculated_disruption"
    )

    resultDF.show(truncate = false)

    resultDF.printSchema()

    checkAnswer(
      resultDF.selectExpr(
        "calculated_disruption as result"
      ),
      spark.sql("select cast(-2.0 as double) as result")
    )

  }

}