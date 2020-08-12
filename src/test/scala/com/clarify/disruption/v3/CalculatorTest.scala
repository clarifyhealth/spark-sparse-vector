package com.clarify.disruption.v3

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types.DataTypes

class CalculatorTest extends QueryTest with SparkSessionTestWrapper {
  test("direct test max delta at beginning") {
    val calculator = new Calculator()
    val data: Seq[Double] = Seq(2.0, 1.6666666666666665, 1.3333333333333333, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    val result: Option[Double] = calculator.call(data)
    assert(result.isDefined)
    assert(result.get == -0.3333333333333335)
  }
  test("direct test max delta in the middle") {
    val calculator = new Calculator()
    val data: Seq[Double] = Seq(2.0, 2.0, 0.0, 2.0, 0.0 , 2.0, 1.6666666666666665, 1.3333333333333333, 1.0, 1.0, 1.0, 1.0)
    val result: Option[Double] = calculator.call(data)
    assert(result.isDefined)
    assert(result.get == -2.0)
  }
  test("direct test max delta at the end") {
    val calculator = new Calculator()
    val data: Seq[Double] = Seq(2.0, 2.0, 1.5, 2.0, 1.5 , 2.0, 1.6666666666666665, 1.3333333333333333, 2.0, 2.0, 2.0, 1.0)
    val result: Option[Double] = calculator.call(data)
    assert(result.isDefined)
    assert(result.get == -1.0)
  }
  test("direct test quarterly max delta at the beginning") {
    val calculator = new Calculator()
    val data: Seq[Double] = Seq(3.0, 1.0, 1.0, 1.0)
    val result: Option[Double] = calculator.call(data)
    assert(result.isDefined)
    assert(result.get == -2.0)
  }
  test("direct test quarterly max delta at the end") {
    val calculator = new Calculator()
    val data: Seq[Double] = Seq(3.0, 3.0, 3.0, 1.0)
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

  test("null handling test") {

    val testDF = spark.sql(
      s"select cast(array(2.0, 2, 0, 2, 0, null, 1.6666666666666665, 1.3333333333333333, 1.0, 1.0, 1.0, 1.0) as array<double>) as data"
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

    //    checkAnswer(
    //      resultDF.selectExpr(
    //        "calculated_disruption as result"
    //      ),
    //      spark.sql("select cast(-2.0 as double) as result")
    //    )

  }

  test("NaN handling test") {

    val testDF = spark.sql(
      s"select cast(array(2.0, 2, 0, 2, 0, Double.NaN, 1.6666666666666665, 1.3333333333333333, 1.0, 1.0, 1.0, 1.0) as array<double>) as data"
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

//    checkAnswer(
//      resultDF.selectExpr(
//        "calculated_disruption as result"
//      ),
//      spark.sql("select cast(-2.0 as double) as result")
//    )

  }

}