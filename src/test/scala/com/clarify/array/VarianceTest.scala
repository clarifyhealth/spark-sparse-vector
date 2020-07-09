package com.clarify.array

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types.{DataTypes}

class VarianceTest extends QueryTest with SparkSessionTestWrapper {
  test("basic test") {
    val testDF = spark.sql(
      "select cast(array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0) as array<double>) as data"
    )

    spark.udf.register(
      "array_variance",
      new Variance(),
      DataTypes.DoubleType
    )

    val resultDF = testDF.selectExpr(
      "data",
      "array_variance(data) as array_variance"
    )
    checkAnswer(
      resultDF.select("array_variance"),
      spark.sql(
        "select cast(2.9166666666666665 as double) as array_variance"
      )
    )

  }

}
