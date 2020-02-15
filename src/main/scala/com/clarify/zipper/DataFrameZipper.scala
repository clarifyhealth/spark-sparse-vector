package com.clarify.zipper

import java.util

import scala.collection.JavaConversions._
import com.clarify.Helpers
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object DataFrameZipper {

  def zipDataFrames(sql_ctx: SQLContext, view1: String, view2: String,
                    result_view: String): Boolean = {

    val a: DataFrame = sql_ctx.table(view1)
    val b: DataFrame = sql_ctx.table(view2)
    Helpers.log(f"Zipping data frames: $view1 <- $view2")
    val result_df: DataFrame = _zipDataFramesAllColumns(a, b)
    result_df.createOrReplaceTempView(result_view)
    Helpers.log(f"Finished zipping data frames: $view1 with $view2")
    true
  }

  def zipDataFramesSingleColumn(sql_ctx: SQLContext, view1: String, view2: String,
                                result_view: String, column_index: Int): Boolean = {

    val a: DataFrame = sql_ctx.table(view1)
    val b: DataFrame = sql_ctx.table(view2)
    Helpers.log(f"Zipping data frames: $view1 <- $view2")
    val result_df: DataFrame = _zipDataFramesSingleColumn(a, b, column_index)
    result_df.createOrReplaceTempView(result_view)
    Helpers.log(f"Finished zipping data frames: $view1 with $view2")
    true
  }

  def zipDataFramesList(sql_ctx: SQLContext, view1: String, views: util.ArrayList[String],
                        result_view: String, column_index: Int,
                        chunk_number: Int, total_chunks: Int): Boolean = {

    val a: DataFrame = sql_ctx.table(view1)
    val len = views.size()
    var i: Int = 0
    var left_rdd = a.rdd
    var left_schema_fields = a.schema.fields
    for (view_to_zip <- Helpers.getSeqString(views)) {
      i = i + 1
      val b: DataFrame = sql_ctx.table(view_to_zip)
      Helpers.log(f"Zipping data frame $i of $len (chunk $chunk_number of $total_chunks): $view1 <- $view_to_zip")

      left_rdd = left_rdd.zip(b.rdd).map {
        case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq :+ rowRight(column_index))
      }

      left_schema_fields = left_schema_fields :+ b.schema.fields(column_index)
    }

    val result_df: DataFrame = a.sqlContext.createDataFrame(left_rdd, StructType(left_schema_fields))

    Helpers.log(f"Finished zipping data frames: $view1 with $views")
    result_df.createOrReplaceTempView(result_view)
    Helpers.log(f"Returning result into $result_view")

    true
  }


  def zipDataFramesListMultipleColumns(sql_ctx: SQLContext, view1: String, views: util.ArrayList[String],
                                       result_view: String, column_indexes: util.ArrayList[Int],
                                       chunk_number: Int, total_chunks: Int): Boolean = {

    val a: DataFrame = sql_ctx.table(view1)
    val len: Int = views.size()
    var i: Int = 0
    var left_rdd: RDD[Row] = a.rdd
    var left_schema_fields: Array[StructField] = a.schema.fields
    for (view_to_zip: String <- Helpers.getSeqString(views)) {
      i = i + 1
      val b: DataFrame = sql_ctx.table(view_to_zip)
      Helpers.log(f"Zipping data frame $i of $len (chunk $chunk_number of $total_chunks): $view1 <- $view_to_zip, columns=$column_indexes")
      val right_rdd: RDD[Row] = b.rdd
      column_indexes.foreach {
        column_index: Int =>
          if (column_index < b.columns.length) {
            left_rdd = left_rdd.zip(right_rdd).map {
              case (rowLeft: Row, rowRight: Row) => Row.fromSeq(rowLeft.toSeq :+ rowRight(column_index))
            }

            left_schema_fields = left_schema_fields :+ b.schema.fields(column_index)
          }
      }
      true
    }

    val result_df: DataFrame = a.sqlContext.createDataFrame(left_rdd, StructType(left_schema_fields))

    Helpers.log(f"Finished zipping data frames: $view1 with $views")
    result_df.createOrReplaceTempView(result_view)
    Helpers.log(f"Returning result into $result_view")

    true
  }

  def zipDataFramesListMultipleColumnsRange(sql_ctx: SQLContext, view1: String, views: util.ArrayList[String],
                                            result_view: String, begin_column_index: Int, end_column_index: Int,
                                            chunk_number: Int, total_chunks: Int): Boolean = {

    val a: DataFrame = sql_ctx.table(view1)
    val len: Int = views.size()
    var i: Int = 0
    var left_rdd: RDD[Row] = a.rdd
    var left_schema_fields: Array[StructField] = a.schema.fields
    for (view_to_zip <- Helpers.getSeqString(views)) {
      i = i + 1
      val b: DataFrame = sql_ctx.table(view_to_zip)
      Helpers.log(f"Zipping data frame $i of $len (chunk $chunk_number of $total_chunks): $view1 <- $view_to_zip, begin_column_index=$begin_column_index, end_column_index=$end_column_index")
      val right_rdd: RDD[Row] = b.rdd
      val safe_end_column_index = if (end_column_index > b.columns.length - 1) b.columns.length - 1 else end_column_index
//      Helpers.log(f"zip begin_column_index=$begin_column_index to safe_end_column_index=$safe_end_column_index")
      left_rdd = left_rdd.zip(right_rdd).map {
        case (rowLeft: Row, rowRight: Row) => Row.fromSeq(rowLeft.toSeq ++ rowRight.toSeq.slice(begin_column_index, safe_end_column_index + 1))
      }

      left_schema_fields = left_schema_fields ++ b.schema.fields.slice(begin_column_index, safe_end_column_index + 1)
      true
    }
    val result_df: DataFrame = a.sqlContext.createDataFrame(left_rdd, StructType(left_schema_fields))

    Helpers.log(f"Finished zipping data frames: $view1 with $views")
    result_df.createOrReplaceTempView(result_view)
    Helpers.log(f"Returning result into $result_view")

    true

  }

  def _zipDataFramesSingleColumn(a: DataFrame, b: DataFrame, column_index: Int): DataFrame = {
    // Merge rows
    val rows = a.rdd.zip(b.rdd).map {
      case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq :+ rowRight(column_index))
    }

    // Merge schemas
    val schema = StructType(a.schema.fields :+ b.schema.fields(column_index))

    // Create new data frame
    val ab: DataFrame = a.sqlContext.createDataFrame(rows, schema)
    ab
  }

  def _zipDataFramesAllColumns(a: DataFrame, b: DataFrame): DataFrame = {
    // Merge rows
    val rows = a.rdd.zip(b.rdd).map {
      case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq ++ rowRight.toSeq)
    }

    // Merge schemas
    val schema = StructType(a.schema.fields ++ b.schema.fields)

    // Create new data frame
    val ab: DataFrame = a.sqlContext.createDataFrame(rows, schema)
    ab
  }

}
