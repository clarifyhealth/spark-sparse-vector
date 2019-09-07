package com.clarify.zipper

import java.util

import com.clarify.Helpers
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object DataFrameZipper {

  def zipDataFrames(sql_ctx: SQLContext, view1: String, view2: String, result_view: String, column_index: Int): Boolean = {

    val a: DataFrame = sql_ctx.table(view1)
    val b: DataFrame = sql_ctx.table(view2)
    Helpers.log(f"Zipping data frames: $view1 with $view2")
    val result_df: DataFrame = _zipDataFrames(a, b, column_index)
    result_df.createOrReplaceTempView(result_view)
    Helpers.log(f"Finished zipping data frames: $view1 with $view2")
    true
  }

  def zipDataFramesList(sql_ctx: SQLContext, view1: String, views: util.ArrayList[String], result_view: String, column_index: Int): Boolean = {

    val a: DataFrame = sql_ctx.table(view1)
    var result_df: DataFrame = a
    val len = views.size()
    var i: Int = 0
    for (view_to_zip <- Helpers.getSeqString(views)) {
      i = i + 1
      val b: DataFrame = sql_ctx.table(view_to_zip)
      Helpers.log(f"Zipping data frames $i of $len: $view1 with $view_to_zip")
      result_df = _zipDataFrames(result_df, b, column_index)
    }
    Helpers.log(f"Finished zipping data frames: $view1 with $views")
    result_df.createOrReplaceTempView(result_view)
    Helpers.log(f"Returning result into $result_view")

    true
  }

  def _zipDataFrames(a: DataFrame, b: DataFrame, column_index: Int): DataFrame = {
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

}
