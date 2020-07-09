package com.clarify.array

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types.{DataType, DataTypes}

class ConvolveTest extends QueryTest with SparkSessionTestWrapper {
  test("basic test") {
    val testDF = spark.sql(
      "select cast(array(2.0, 3.0, 4.0, 5.0) as array<double>) as data, " +
        "cast(array(1.0, 2.0) as array<double>) as kernelEven, " +
        "cast(array(1.0, 2.0, 3.0) as array<double>) as kernelOdd"
    )

    spark.udf.register(
      "array_convolve",
      new Convolve(),
      DataTypes.createArrayType(DataTypes.DoubleType)
    )

    val resultDF = testDF.selectExpr(
      "data",
      "array_convolve(data,kernelEven) as convolveEven",
      "array_convolve(data,kernelOdd) as convolveOdd"
    )
    checkAnswer(
      resultDF.select("convolveEven", "convolveOdd"),
      spark.sql(
        "select cast(array(7.0, 10.0, 13.0) as array<double>) as kernelEven, " +
          "cast(array(16.0, 22.0) as array<double>) as kernelOdd"
      )
    )

  }

}
