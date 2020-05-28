package com.clarify.custom

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1

class AggregateNestedMap extends UDF1[Seq[Map[String, Row]], Map[String, Row]] {

  override def call(
      entries: Seq[Map[String, Row]]
  ): Map[String, Row] = {
    entries.flatMap { item =>
      item.filter { case (_, value: Row) => !value.isNullAt(0) }
    }.toMap
  }
}
