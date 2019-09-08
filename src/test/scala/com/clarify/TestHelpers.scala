package com.clarify

import org.apache.spark.sql.SparkSession

object TestHelpers {

  def clear_tables(spark_session: SparkSession): Boolean = {

    val tables = spark_session.catalog.listTables("default").collect()

    for (table <- tables) {
      print(s"clear_tables() dropping table/view: ${table.name}")

      spark_session.sql(f"DROP TABLE IF EXISTS default.${table.name}")
      spark_session.sql(f"DROP VIEW IF EXISTS default.${table.name}")
      spark_session.sql(f"DROP VIEW IF EXISTS ${table.name}")
    }

    spark_session.catalog.clearCache()
    true
  }

}
