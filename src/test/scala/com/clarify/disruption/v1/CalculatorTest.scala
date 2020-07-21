package com.clarify.disruption.v1

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types.DataTypes
import com.clarify.sparse_vectors.SparkSessionTestWrapper

import scala.util.Random

class CalculatorTest extends QueryTest with SparkSessionTestWrapper {
  test("basic test") {

    val test_data = Seq.fill(20)((Random.nextDouble() * 2) + 20) ++ Seq.fill(
      12
    )(
      (Random.nextDouble() * 2) + 5
    )

    val test_data_cast_double = test_data.map(x => s"cast(${x} as double)")

    val testDF = spark.sql(
      s"select array(${test_data_cast_double.mkString(",")})  as data"
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
    checkAnswer(
      resultDF.selectExpr(
        "case when calculated_disruption between 5 and 6 then 1 else 0 end as result"
      ),
      spark.sql("select 1 as result")
    )

  }

}
