package com.clarify.disruption.v2

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types.DataTypes

class CalculatorTest extends QueryTest with SparkSessionTestWrapper {
  test("basic test") {

    val testDF = spark.sql(
      s"select cast(array(3.0, 6.0, 17.0, 3.0, -19.0, 2.0) as array<double>) as data"
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
      spark.sql("select cast(21.0 as double) as result")
    )

  }

}
