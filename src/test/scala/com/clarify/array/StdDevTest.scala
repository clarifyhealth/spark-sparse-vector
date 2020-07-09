package com.clarify.array

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types.DataTypes

class StdDevTest extends QueryTest with SparkSessionTestWrapper {
  test("basic test") {
    val testDF = spark.sql(
      "select cast(array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0) as array<double>) as data"
    )

    spark.udf.register(
      "array_stddev",
      new StdDev(),
      DataTypes.DoubleType
    )

    val resultDF = testDF.selectExpr(
      "data",
      "array_stddev(data) as array_stddev"
    )
    checkAnswer(
      resultDF.select("array_stddev"),
      spark.sql(
        "select cast(1.707825127659933 as double) as array_stddev"
      )
    )

  }

}
