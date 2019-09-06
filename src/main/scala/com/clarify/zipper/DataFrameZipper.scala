package com.clarify.zipper

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object DataFrameZipper {

  def zipDataFrames(sql_ctx: SQLContext, view1: String, view2: String, result_view: String): Boolean = {

    val a: DataFrame = sql_ctx.table(view1)
    val b: DataFrame = sql_ctx.table(view2)

    val result_df: DataFrame = _zipDataFrames(a, b)
    result_df.createOrReplaceTempView(result_view)
    true
  }

  def _zipDataFrames(a: DataFrame, b: DataFrame): DataFrame = {
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
