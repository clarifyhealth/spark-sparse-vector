package com.clarify.custom

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

class AggregateNestedMap
    extends UDF1[Seq[Map[String, GenericRowWithSchema]], Map[String, Row]] {

  override def call(
      entries: Seq[Map[String, GenericRowWithSchema]]
  ): Map[String, Row] = {
    entries.flatMap { item =>
      item.filter {
        case (_, rowValue: GenericRowWithSchema) =>
          if (rowValue.schema.names.contains("predicted_value")) {
            !rowValue.isNullAt(rowValue.fieldIndex("predicted_value"))
          } else if (rowValue.schema.names.contains("contrib_intercept")) {
            !rowValue.isNullAt(rowValue.fieldIndex("contrib_intercept"))
          } else {
            !rowValue.anyNull
          }
      }
    }.toMap
  }
}
