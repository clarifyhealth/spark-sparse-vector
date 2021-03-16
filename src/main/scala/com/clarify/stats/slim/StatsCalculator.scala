package com.clarify.stats.slim

import java.util

import com.clarify.Helpers
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object StatsCalculator {

  def create_statistics(
      sql_ctx: SQLContext,
      view: String,
      record_count: Int,
      sample_record_count: Int,
      columns_to_include: util.ArrayList[String],
      columns_to_histogram: util.ArrayList[String],
      result_view: String
  ): Boolean = {
    create_statistics(
      sql_ctx,
      view,
      record_count.toLong,
      sample_record_count,
      columns_to_include,
      columns_to_histogram,
      result_view
    )
  }

  def create_statistics(
      sql_ctx: SQLContext,
      view: String,
      record_count: Long,
      sample_record_count: Int,
      columns_to_include: util.ArrayList[String],
      columns_to_histogram: util.ArrayList[String],
      result_view: String
  ): Boolean = {

    sql_ctx.sparkContext.setJobDescription(f"statistics for $view")
    val loaded_df: DataFrame = sql_ctx.table(view)
    val result_df: DataFrame = _create_statistics(
      loaded_df,
      record_count,
      sample_record_count,
      Helpers.getSeqString(columns_to_include),
      Helpers.getSeqString(columns_to_histogram),
      view
    )
    result_df.createOrReplaceTempView(result_view)
    true
  }

  def _create_statistics(
      loaded_df: DataFrame,
      record_count: Long,
      sample_record_count: Int,
      columns_to_include: Seq[String],
      columns_to_histogram: Seq[String],
      view: String
  ): DataFrame = {

    val invalid_column_types = Seq(ArrayType, MapType, StructType)

    val normal_columns: Seq[(String, DataType)] =
      loaded_df.schema
        .filter(
          x => columns_to_include.isEmpty || columns_to_include.contains(x.name)
        )
        .filter(x => !invalid_column_types.contains(x.dataType))
        .map(x => (x.name, x.dataType))

    Helpers.log(f"Calculating statistics for $view")

    val isNumeric: DataType => Boolean = (temp: DataType) => {
      val out = temp match {
        case _: ShortType | _: IntegerType | _: LongType | _: FloatType |
            _: DoubleType | _: DecimalType =>
          true
        case _ => false
      }
      out
    }

    for (normal_column <- normal_columns) {
      val column_name = normal_column._1
      val data_type = normal_column._2
      val data_type_name = normal_column._2.toString

      var my_result: DataFrame = null
      // Helpers.log(f"evaluating column $column_name $data_type_name $normal_column")

      if (isNumeric(data_type)) {
        // Helpers.log(f"Processing numerical column ${normal_column._1} $normal_column")
        //noinspection SpellCheckingInspection
        my_result = loaded_df.select(
          lit(column_name).alias("column_name"),
          lit(data_type_name).alias("data_type"),
          lit(record_count).cast(LongType).alias("total_count"),
          round(
            min(col(column_name))
              .cast(DoubleType),
            3
          ).alias("sample_min"),
          round(
            max(column_name)
              .cast(DoubleType),
            3
          ).alias("sample_max"),
          round(
            mean(column_name)
              .cast(DoubleType),
            3
          ).alias("sample_mean")
        )
      } else {
        // Helpers.log(f"Processing non-numerical column ${normal_column._1} $normal_column")
        //noinspection SpellCheckingInspection
        my_result = loaded_df.select(
          lit(column_name).alias("column_name"),
          lit(data_type_name).alias("data_type"),
          lit(record_count).cast(LongType).alias("total_count"),
          lit(null).cast(DoubleType).alias("sample_min"),
          lit(null).cast(DoubleType).alias("sample_max"),
          lit(null).cast(DoubleType).alias("sample_mean")
        )
      }

    // execute in parallel
    val my_result_list: Seq[Row] =
      my_result_data_frames.par.map(df => df.first()).seq
    // val result_statistics_df: DataFrame = my_result_data_frames.reduce((a, b) => a.union(b))
    val statistics_schema = StructType(
      Array(
        StructField("column_name", StringType, nullable = false),
        StructField("data_type", StringType, nullable = false),
        StructField("total_count", LongType, nullable = false),
        StructField("sample_min", DoubleType),
        StructField("sample_max", DoubleType),
        StructField("sample_mean", DoubleType)
      )
    )
    val result_statistics_df: DataFrame =
      loaded_df.sqlContext.createDataFrame(
        loaded_df.sqlContext.sparkContext.makeRDD(my_result_list),
        statistics_schema
      )
    Helpers.log(f"Finished calculating statistics for $view")

    result_statistics_df
  }
    result_data_frame
  }
}
