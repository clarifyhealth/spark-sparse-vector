package com.clarify.custom

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  MapType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{QueryTest, SaveMode}

class AggregateNestedMapTest extends QueryTest with SparkSessionTestWrapper {

  test("aggregate nested map test") {
    spark.sharedState.cacheManager.clearCache()
    val inner = StructType(
      List(
        StructField("model_id", StringType),
        StructField("valid", IntegerType),
        StructField("rmse", DoubleType),
        StructField("value", DoubleType)
      )
    )

    val metric = StructType(
      List(StructField("metric", MapType(StringType, inner)))
    )

    val schema =
      StructType(
        List(
          StructField("ccg_id", StringType),
          StructField("predictions", metric)
        )
      )

    val df1 = spark.read.schema(schema) json (getClass
      .getResource("/nested_json/another_sytax.json")
      .getPath)
    df1.coalesce(1).write.mode(SaveMode.Overwrite).parquet("/tmp/test_data_1")

    val df2 = spark.read.schema(schema) json (
      getClass.getResource("/nested_json/another_sytax_2.json").getPath
    )
    df2.coalesce(1).write.mode(SaveMode.Overwrite).parquet("/tmp/test_data_2")

    val main_df = spark.read
      .option("mergeSchema", "true")
      .format("parquet")
      .load("/tmp/test_data_1", "/tmp/test_data_2")

    main_df.createOrReplaceTempView("my_table")

    spark.udf.register(
      "aggregateNestedMap",
      new AggregateNestedMap(),
      MapType(StringType, inner)
    )

    val out_df =
      spark.sql(
        "select ccg_id, aggregateNestedMap(collect_list(predictions.metric)) as temp from my_table group by ccg_id"
      )

    out_df.show(truncate = false)

  }

}
