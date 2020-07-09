package com.clarify.array

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types.{DataTypes}
import com.clarify.sparse_vectors.SparkSessionTestWrapper

class ConvolveTest extends QueryTest with SparkSessionTestWrapper {
  test("basic test") {
    val testDF = spark.sql(
      "select cast(array(2.0, 3.0, 4.0, 5.0) as array<double>) as data"
    )

    spark.udf.register(
      "array_convolve",
      new Convolve(),
      DataTypes.createArrayType(DataTypes.DoubleType)
    )

    val resultDF = testDF.selectExpr(
      "data",
      "array_convolve(data,2) as convolveEven",
      "array_convolve(data,3) as convolveOdd"
    )
    checkAnswer(
      resultDF.select("convolveEven", "convolveOdd"),
      spark.sql(
        "select cast(array(2.5, 3.5, 4.5) as array<double>) as convolveEven, " +
          "cast(array(3.0, 3.9999999999999996) as array<double>) as convolveOdd"
      )
    )

  }

}
