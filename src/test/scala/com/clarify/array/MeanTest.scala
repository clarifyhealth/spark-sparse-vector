package com.clarify.array

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types.{DataTypes}

class MeanTest extends QueryTest with SparkSessionTestWrapper {
  test("basic test") {
    val testDF = spark.sql(
      "select cast(array(0.0, 1.0, 2.0, 3.0, 4.0, 5.0) as array<double>) as data"
    )

    spark.udf.register(
      "array_mean",
      new Mean(),
      DataTypes.DoubleType
    )

    val resultDF = testDF.selectExpr(
      "data",
      "array_mean(data) as array_mean"
    )
    checkAnswer(
      resultDF.select("array_mean"),
      spark.sql(
        "select cast(2.5 as double) as array_mean"
      )
    )

  }

}
