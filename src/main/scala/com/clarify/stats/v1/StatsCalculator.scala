package com.clarify.stats.v1

import java.util

import com.clarify.Helpers
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

object StatsCalculator {

  def create_statistics(sql_ctx: SQLContext,
                        view: String,
                        record_count: Int,
                        sample_record_count: Int,
                        columns_to_include: util.ArrayList[String],
                        columns_to_histogram: util.ArrayList[String],
                        result_view: String): Boolean = {

    val loaded_df: DataFrame = sql_ctx.table(view)
    val result_df: DataFrame = _create_statistics(loaded_df, record_count, sample_record_count,
      Helpers.getSeqString(columns_to_include),
      Helpers.getSeqString(columns_to_histogram),
      view)
    result_df.createOrReplaceTempView(result_view)
    true
  }

  def _create_statistics(loaded_df: DataFrame,
                         record_count: Int,
                         sample_record_count: Int,
                         columns_to_include: Seq[String],
                         columns_to_histogram: Seq[String],
                         view: String): DataFrame = {

    val invalid_column_types = Seq(ArrayType, MapType, StructType)

    val normal_columns: Seq[(String, DataType)] =
      loaded_df.schema
        .filter(x => columns_to_include.isEmpty || columns_to_include.contains(x.name))
        .filter(x => !invalid_column_types.contains(x.dataType))
        .map(x => (x.name, x.dataType))

    var my_result_data_frames: Seq[DataFrame] = Seq()
    Helpers.log(f"Calculating histograms for $view columns: $columns_to_histogram")
    // this returns List[column_name, List[(value. value_count)]
    val histogram_list_all_columns: Seq[(String, Seq[(String, Double)])] = _create_histogram_array(
      columns_to_histogram,
      loaded_df)

    Helpers.log(f"Finished calculating histograms for $view columns: $columns_to_histogram")

    Helpers.log(f"Calculating statistics for $view")

    val numerical_column_types = Seq(ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)

    for (normal_column <- normal_columns) {
      val column_name = normal_column._1
      val data_type = normal_column._2
      val data_type_name = normal_column._2.toString

      var my_result: DataFrame = null
      // Helpers.log(f"evaluating column $column_name $data_type_name $normal_column")

      if (numerical_column_types.contains(data_type)) {
        // Helpers.log(f"Processing numerical column ${normal_column._1} $normal_column")
        //noinspection SpellCheckingInspection
        my_result = loaded_df.select(
          lit(column_name).alias("column_name"),
          lit(data_type_name).alias("data_type"),
          lit(record_count).cast(LongType).alias("total_count"),
          lit(sample_record_count).cast(IntegerType).alias("sample_count"),
          countDistinct(col(column_name)).cast(IntegerType).alias("sample_count_distinct"),
          round(
            count(
              when(
                isnull(col(column_name)),
                lit(1)
              )
            ) * 100 / sample_record_count, 3
          )
            .cast(DoubleType)
            .alias("sample_percent_null"),
          round(
            count(
              when(
                col(column_name) === lit(0),
                lit(1)
              )
            ) * 100 / sample_record_count, 3
          )
            .cast(DoubleType)
            .alias("sample_percent_zero"),
          round(
            count(
              when(
                col(column_name) < lit(0),
                lit(1))) * 100 / sample_record_count, 3
          )
            .cast(DoubleType)
            .alias("sample_percent_less_than_zero"),
          // now create the "five number summary":
          // https://www.statisticshowto.datasciencecentral.com/how-to-find-a-five-number-summary-in-statistics/
          round(
            min(col(column_name))
              .cast(DoubleType), 3)
            .alias("sample_min"),
          // https://stackoverflow.com/questions/31432843/how-to-find-median-and-quantiles-using-spark
          round(
            expr(f"approx_percentile($column_name, 0.25, 100)")
              .cast(DoubleType),
            3)
            .alias("sample_q_1"),
          round(
            expr(f"approx_percentile($column_name, 0.5, 100)")
              .cast(DoubleType),
            3)
            .alias("sample_median"),
          round(
            expr(f"approx_percentile($column_name, 0.75, 100)")
              .cast(DoubleType), 3)
            .alias("sample_q_3"),
          round(
            max(column_name)
              .cast(DoubleType), 3)
            .alias("sample_max"),
          round(
            mean(column_name)
              .cast(DoubleType), 3)
            .alias("sample_mean"),
          round(
            stddev_samp(column_name)
              .cast(DoubleType), 3)
            .alias("sample_stddev"),
          lit(null).cast(StringType).alias("top_value_1"),
          lit(null).cast(DoubleType).alias("top_value_percent_1"),
          lit(null).cast(StringType).alias("top_value_2"),
          lit(null).cast(DoubleType).alias("top_value_percent_2"),
          lit(null).cast(StringType).alias("top_value_3"),
          lit(null).cast(DoubleType).alias("top_value_percent_3"),
          lit(null).cast(StringType).alias("top_value_4"),
          lit(null).cast(DoubleType).alias("top_value_percent_4"),
          lit(null).cast(StringType).alias("top_value_5"),
          lit(null).cast(DoubleType).alias("top_value_percent_5")
        )
      }
      else {
        Helpers.log(f"Processing non-numerical column ${normal_column._1} $normal_column")
        //noinspection SpellCheckingInspection
        my_result = loaded_df.select(
          lit(column_name).alias("column_name"),
          lit(data_type_name).alias("data_type"),
          lit(record_count).cast(LongType).alias("total_count"),
          lit(sample_record_count).cast(IntegerType).alias("sample_count"),
          countDistinct(col(column_name))
            .cast(IntegerType)
            .alias("sample_count_distinct"),
          round(
            count(
              when(
                isnull(col(column_name)),
                lit(1)
              )
            ) * 100 / sample_record_count, 3
          ).alias("sample_percent_null"),
          lit(null).cast(DoubleType).alias("sample_percent_zero"),
          lit(null).cast(DoubleType).alias("sample_percent_less_than_zero"),
          // now create the "five number summary":
          // https://www.statisticshowto.datasciencecentral.com/how-to-find-a-five-number-summary-in-statistics/
          lit(null).cast(DoubleType).alias("sample_min"),
          // https://stackoverflow.com/questions/31432843/how-to-find-median-and-quantiles-using-spark
          lit(null).cast(DoubleType).alias("sample_q_1"),
          lit(null).cast(DoubleType).alias("sample_median"),
          lit(null).cast(DoubleType).alias("sample_q_3"),
          lit(null).cast(DoubleType).alias("sample_max"),
          lit(null).cast(DoubleType).alias("sample_mean"),
          lit(null).cast(DoubleType).alias("sample_stddev"),
          lit(null).cast(StringType).alias("top_value_1"),
          lit(null).cast(DoubleType).alias("top_value_percent_1"),
          lit(null).cast(StringType).alias("top_value_2"),
          lit(null).cast(DoubleType).alias("top_value_percent_2"),
          lit(null).cast(StringType).alias("top_value_3"),
          lit(null).cast(DoubleType).alias("top_value_percent_3"),
          lit(null).cast(StringType).alias("top_value_4"),
          lit(null).cast(DoubleType).alias("top_value_percent_4"),
          lit(null).cast(StringType).alias("top_value_5"),
          lit(null).cast(DoubleType).alias("top_value_percent_5")
        )
      }

      // now fill in the histogram
      if (columns_to_histogram contains column_name) {
        val histogram_array_tuple: (String, Seq[(String, Double)]) = histogram_list_all_columns.filter(x => x._1 == column_name).head
        val histogram_array: Seq[(String, Double)] = histogram_array_tuple._2
        var i: Int = 0
        for (histogram <- histogram_array) {
          val histogram_percent: Double = histogram._2 * 100 / sample_record_count
          my_result = my_result
            .withColumn(f"top_value_${i + 1}", lit(histogram._1))
            .withColumn(f"top_value_percent_${i + 1}", round(lit(histogram_percent), 3))
          i += 1
        }
      }
      my_result_data_frames = my_result_data_frames :+ my_result
      // my_result_list = my_result_list :+ my_result.first()
    }

    // val my_result_list: Seq[DataFrame] = my_result_data_frames.par.map(df => df).seq
    val result_statistics_df: DataFrame = my_result_data_frames.reduce((a, b) => a.union(b))
    //    val result_statistics_df: DataFrame =
    //      loaded_df.sqlContext.createDataFrame(loaded_df.sqlContext.sparkContext.makeRDD(my_result_list), statistics_schema)
    Helpers.log(f"Finished calculating statistics for $view")

    result_statistics_df
  }

  def _create_histogram_array(columns_to_histogram: Seq[String],
                              loaded_df: DataFrame): Seq[(String, Seq[(String, Double)])] = {
    var result: Seq[(String, Seq[(String, Double)])] = Seq()
    for (column_name <- columns_to_histogram) {
      result = result :+ (column_name, _calculate_histogram_array_for_column(column_name, loaded_df))
    }
    result
  }

  def create_histogram(loaded_df: DataFrame, normal_columns: Seq[String],
                       record_count: Int, sample_record_count: Int,
                       columns_to_histogram: Seq[String]): DataFrame = {

    val histogram_schema = StructType(
      Array(
        StructField("column_name", StringType, nullable = false),
        StructField("data_type", StringType, nullable = false),
        StructField("total_count", LongType, nullable = false),
        StructField("sample_count", IntegerType, nullable = false),
        StructField("sample_histogram", StringType)
      )
    )

    val empty_rdd: RDD[Row] = loaded_df.sqlContext.sparkContext.makeRDD(Seq[Row]())
    val result_histogram_df: DataFrame = loaded_df.sqlContext.sparkSession.createDataFrame(empty_rdd, histogram_schema)

    result_histogram_df
  }

  def _calculate_histogram_array_for_column(column_name: String,
                                            loaded_df: DataFrame): Seq[(String, Double)] = {
    val result_data_frame: DataFrame = loaded_df
      .select(column_name)
      .groupBy(column_name)
      .agg(
        count("*").alias("count")
      )
      .sort(col("count").desc)
      .limit(5)
      .withColumn("key_plus_value",
        struct(
          col(f"$column_name").cast(StringType).alias("value"),
          col("count").cast(DoubleType).alias("value_count")
        )
      )
      //      .agg(
      //        collect_list("key_plus_value").alias("result")
      //      )
      .select("key_plus_value")

    val histogram: Seq[(String, Double)] =
      result_data_frame
        .rdd
        .map {
          row =>
            Row(
              row.getStruct(0).getAs[String]("value"),
              row.getStruct(0).getAs[Double]("value_count")
            )
        }.collect().map(row => (row.getString(0), row.getDouble(1)))

    histogram
  }

  def _calculate_histogram_for_column(column_name: String, loaded_df: DataFrame): Seq[(String, Int)] = {
    var df_histogram: Dataset[Row] = loaded_df
      .select(column_name)
      .groupBy(column_name)
      .agg(count("*").alias("count"))
      .sort(col("count").desc)
      .limit(5)

    df_histogram = df_histogram.withColumn("key_plus_value",
      concat_ws(":", col(f"{column_name}"), col("count").alias("value_count")))
    df_histogram = df_histogram.agg(concat_ws(",", collect_list("key_plus_value")).alias("result"))

    val histogram: Seq[(String, Int)] =
      df_histogram.collect()(0).asInstanceOf[Seq[(String, Int)]]
    histogram
  }
}
