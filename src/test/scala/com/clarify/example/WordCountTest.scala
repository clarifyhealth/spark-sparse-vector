package com.clarify.example

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}
import com.clarify.sparse_vectors.SparkSessionTestWrapper

class WordCountTest extends QueryTest with SparkSessionTestWrapper {

  ignore("word count") {
    spark.sharedState.cacheManager.clearCache()
    val data = List(Row("Hello this is my favourite test"),
      Row("This is cool"),
      Row("Time for some performance test"),
      Row("Clarify Tera Team"),
      Row("Doing things right and doing the right thing"),
      Row("Oh Model fit and predict"))

    val fields = List(
      StructField("input_col", StringType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table")

    // df.show()

    val wordCount = new WordCount().call _

    spark.udf.register("wordCount", wordCount)

    val out_df = spark.sql("select input_col, wordCount(input_col) as count from my_table")

    checkAnswer(out_df.selectExpr("count"), Seq(Row(3), Row(3), Row(5), Row(5), Row(6), Row(8)))

    // out_df.show()

    assert(6 == df.count())

  }

}

