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
        case (_, value: GenericRowWithSchema) =>
          if (value.schema.names.contains("predicted_value") || value.schema.names
                .contains("sigma")) {
            !value.anyNull
          } else {
            !value.isNullAt(0)
          }
      }
    }.toMap
  }
}
