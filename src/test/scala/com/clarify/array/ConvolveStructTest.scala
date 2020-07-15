package com.clarify.array

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types.DataTypes

class ConvolveStructTest extends QueryTest with SparkSessionTestWrapper {
  test("basic test") {
    val testDF = spark.sql(
      "select array(struct(0 as interval_index , cast(2.0 as double) as value, false as is_null)," +
        "struct(1 as interval_index , cast(3.0 as double) as value, false as is_null)," +
        "struct(2 as interval_index , cast(4.0 as double) as value, false as is_null)," +
        "struct(3 as interval_index , cast(5.0 as double) as value, false as is_null)) as data"
    )

    spark.udf.register(
      "array_convolve_struct",
      new ConvolveStruct(),
      DataTypes.createArrayType(DataTypes.DoubleType)
    )

    val resultDF = testDF.selectExpr(
      "data",
      "array_convolve_struct(data,3) as convolveTest"
    )

    checkAnswer(
      resultDF.select("convolveTest"),
      spark.sql(
        "select cast(array(3.0, 3.9999999999999996) as array<double>) as convolveTest"
      )
    )

  }

  test("basic test scenario [1 NaN 3 4 5 6]") {
    val testDF = spark.sql(
      "select array(struct(0 as interval_index , cast(1.0 as double) as value, false as is_null)," +
        "struct(1 as interval_index , cast(0.0 as double) as value, true as is_null)," +
        "struct(2 as interval_index , cast(3.0 as double) as value, false as is_null)," +
        "struct(3 as interval_index , cast(4.0 as double) as value, false as is_null)," +
        "struct(4 as interval_index , cast(5.0 as double) as value, false as is_null)," +
        "struct(5 as interval_index , cast(6.0 as double) as value, false as is_null)) as data"
    )

    spark.udf.register(
      "array_convolve_struct",
      new ConvolveStruct(),
      DataTypes.createArrayType(DataTypes.DoubleType)
    )

    val resultDF = testDF.selectExpr(
      "data",
      "array_convolve_struct(data,3) as convolveTest"
    )

    checkAnswer(
      resultDF.select("convolveTest"),
      spark.sql(
        "select cast(array(2.0, 3.0, 3.9999999999999996, 5.0) as array<double>) as convolveTest"
      )
    )

  }

  test("basic test scenario [1 Nan NaN Nan 3 4 5 6]") {
    val testDF = spark.sql(
      "select array(struct(0 as interval_index , cast(1.0 as double) as value, false as is_null)," +
        "struct(1 as interval_index , cast(0.0 as double) as value, true as is_null)," +
        "struct(2 as interval_index , cast(0.0 as double) as value, true as is_null)," +
        "struct(3 as interval_index , cast(0.0 as double) as value, true as is_null)," +
        "struct(4 as interval_index , cast(3.0 as double) as value, false as is_null)," +
        "struct(5 as interval_index , cast(4.0 as double) as value, false as is_null)," +
        "struct(6 as interval_index , cast(5.0 as double) as value, false as is_null)," +
        "struct(7 as interval_index , cast(6.0 as double) as value, false as is_null)) as data"
    )

    spark.udf.register(
      "array_convolve_struct",
      new ConvolveStruct(),
      DataTypes.createArrayType(DataTypes.DoubleType)
    )

    val resultDF = testDF.selectExpr(
      "data",
      "array_convolve_struct(data,3) as convolveTest"
    )

    checkAnswer(
      resultDF.select("convolveTest"),
      spark.sql(
        "select cast(array(1.6666666666666665, 2.0, 2.333333333333333, 3.0, 3.9999999999999996, 5.0) as array<double>) as convolveTest"
      )
    )

  }

  test("basic test scenario [Nan NaN Nan 3 4 5 6]") {
    val testDF = spark.sql(
      "select array(struct(0 as interval_index , cast(0.0 as double) as value, true as is_null)," +
        "struct(1 as interval_index , cast(0.0 as double) as value, true as is_null)," +
        "struct(2 as interval_index , cast(0.0 as double) as value, true as is_null)," +
        "struct(4 as interval_index , cast(3.0 as double) as value, false as is_null)," +
        "struct(5 as interval_index , cast(4.0 as double) as value, false as is_null)," +
        "struct(6 as interval_index , cast(5.0 as double) as value, false as is_null)," +
        "struct(7 as interval_index , cast(6.0 as double) as value, false as is_null)) as data"
    )

    spark.udf.register(
      "array_convolve_struct",
      new ConvolveStruct(),
      DataTypes.createArrayType(DataTypes.DoubleType)
    )

    val resultDF = testDF.selectExpr(
      "data",
      "array_convolve_struct(data,3) as convolveTest"
    )

    checkAnswer(
      resultDF.select("convolveTest"),
      spark.sql(
        "select cast(array(3.0, 3.0, 3.333333333333333, 3.9999999999999996, 5.0) as array<double>) as convolveTest"
      )
    )

  }

  test("basic test scenario [3 4 5 6 NaN]") {
    val testDF = spark.sql(
      "select array(struct(0 as interval_index , cast(3.0 as double) as value, false as is_null)," +
        "struct(1 as interval_index , cast(4.0 as double) as value, false as is_null)," +
        "struct(2 as interval_index , cast(5.0 as double) as value, false as is_null)," +
        "struct(3 as interval_index , cast(6.0 as double) as value, false as is_null)," +
        "struct(4 as interval_index , cast(0.0 as double) as value, true as is_null)) as data"
    )

    spark.udf.register(
      "array_convolve_struct",
      new ConvolveStruct(),
      DataTypes.createArrayType(DataTypes.DoubleType)
    )

    val resultDF = testDF.selectExpr(
      "data",
      "array_convolve_struct(data,3) as convolveTest"
    )

    checkAnswer(
      resultDF.select("convolveTest"),
      spark.sql(
        "select cast(array(3.9999999999999996, 5.0, 5.666666666666666) as array<double>) as convolveTest"
      )
    )

  }

}
