package com.clarify.prediction_explainer

import com.clarify.sparse_vectors.{CalculatePopulationContribution, FeatureListItem, SparkSessionTestWrapper}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{QueryTest, Row}

import scala.collection.mutable

case class AnimalCaseClass(name: String, gender: String)

class CalculatePopulationContributionTest extends QueryTest with SparkSessionTestWrapper {

  val spark2 = spark

  import spark2.implicits._

  test("get population log odds for simple feature") {
    val population_log_odds_vector = new SparseVector(3, Array(0, 1), Array(0.1, 0.2))
    val feature_list: Seq[FeatureListItem] = Seq(FeatureListItem(0, "foo", "foo"))
    val contribution: Double = new CalculatePopulationContribution()
      .get_population_log_odds_for_feature(population_log_odds_vector, feature_list, 0)
    assert(contribution == 0.1)
  }

  test("get population log odds for non-ohe feature") {
    val population_log_odds_vector = new SparseVector(3, Array(0, 1), Array(0.2, 0.3))
    val feature_list: Seq[FeatureListItem] = Seq(FeatureListItem(0, "foo", "foo"), FeatureListItem(1, "bar", "bar"))
    val contribution: Double = new CalculatePopulationContribution()
      .get_population_log_odds_for_feature(population_log_odds_vector, feature_list, 0)
    assert(contribution == 0.2)
  }

  test("get population log odds for ohe feature") {
    val population_log_odds_vector = new SparseVector(3, Array(0, 1), Array(0.2, 0.3))
    val feature_list: Seq[FeatureListItem] = Seq(FeatureListItem(0, "foo", "foo"), FeatureListItem(1, "bar", "foo"))
    val contribution: Double = new CalculatePopulationContribution()
      .get_population_log_odds_for_feature(population_log_odds_vector, feature_list, 0)
    assert(contribution == 0.5)
  }

  test("calculate pop log odds simple") {
    val ccg_log_odds_vector: SparseVector = new SparseVector(3, Array(0, 1, 2), Array(0.2, 0.3, 0.4))
    val pop_log_odds_vector: SparseVector = new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5))
    val feature_list: Seq[FeatureListItem] = Seq(
      FeatureListItem(0, "foo", "foo"),
      FeatureListItem(1, "bar", "foo"),
      FeatureListItem(2, "moo", "moo")
    )

    val v3 = new CalculatePopulationContribution().sparse_vector_calculate_population_contribution_log_odds(
      ccg_log_odds_vector, pop_log_odds_vector, feature_list)
    assert(v3 == new SparseVector(3, Array(0, 1, 2), Array(0.7, 0.7, 0.5)))
  }

  test("calculate pop log odds with zeros") {
    val ccg_log_odds_vector: SparseVector = new SparseVector(3, Array(0, 2), Array(0.2, 0.4))
    val pop_log_odds_vector: SparseVector = new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5))
    val feature_list: Seq[FeatureListItem] = Seq(
      FeatureListItem(0, "foo", "foo"),
      FeatureListItem(1, "bar", "foo"),
      FeatureListItem(2, "moo", "moo"))

    val v3 = new CalculatePopulationContribution().sparse_vector_calculate_population_contribution_log_odds(
      ccg_log_odds_vector, pop_log_odds_vector, feature_list)
    assert(v3 == new SparseVector(3, Array(0, 2), Array(0.7, 0.5)))

  }
  test("calculate pop log odds data frame simple") {
    spark.sharedState.cacheManager.clearCache()

    val data = Seq(
      (12345L, "Asia", Seq(FeatureListItem(0, "foo", "foo"))),
      (12346L, "Asia", Seq(FeatureListItem(0, "foo", "foo")))
    ).toDF()
    val df = data.toDF("v1", "v2", "feature_list")

    df.show()
    df.printSchema()
  }
  test("calculate pop log odds data frame simple 4") {
    spark.sharedState.cacheManager.clearCache()

    val data = Seq(
      (12345L, "Asia", Seq("foo", "bar")),
      (12346L, "Asia", Seq("foo"))
    ).toDF()
    val df = data.toDF("v1", "v2", "feature_list")

    df.show(truncate = false)
    df.printSchema()

    val my_udf = udf((row: scala.collection.mutable.WrappedArray[java.lang.Object]) => {
      println(row.array(0).getClass)
      val foo = row.array(0).asInstanceOf[String]
      println(foo.getClass)
      println(row.getClass)
      // row.array.foreach{f => println(f)}
      foo
    })

    df.withColumn("dd", my_udf($"feature_list")).show()

  }
  test("calculate pop log odds data frame simple 3") {
    spark.sharedState.cacheManager.clearCache()

    val data = Seq(
      (12345L, "Asia", Seq((0, "foo", "foo"), (1, "bar", "bar"))),
      (12346L, "Asia", Seq((0, "bar", "zoo")))
    ).toDF()
    val df = data.toDF("v1", "v2", "feature_list")

    df.show(truncate = false)
    df.printSchema()

    val my_udf = udf((feature_list_native: scala.collection.mutable.WrappedArray[java.lang.Object]) => {
      println(feature_list_native.getClass)
      val feature_list = feature_list_native.asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]]
      val sqs = feature_list.toSeq
      //      println("sqs")
      //      println(sqs)
      println("sqs-2")
      sqs.foreach { f => println(f(1)) }
      println("sqs-3")
      val features = feature_list.map(x => FeatureListItem(
        feature_index = x(0).asInstanceOf[Int],
        feature_name = x(1).asInstanceOf[String],
        base_feature_name = x(2).asInstanceOf[String]))
      print(features)
      println("sqs-4")
      val g = features.filter(x => x.feature_index == 0).head
      print(g)
      // println(sqs(0)(0))
      val gr = feature_list_native.array(0).asInstanceOf[GenericRowWithSchema]
      val sq = gr.toSeq
      //      println("sq")
      //      println(sq)
      //      println(sq(1))
      //      val foo = feature_list_native.array(0).asInstanceOf[(Int,String,String)]
      //      println(foo.getClass)
      //      println(feature_list_native.getClass)
      // feature_list_native.array.foreach{f => println(f)}
      // sq(1).asInstanceOf[String]
      g.feature_name.asInstanceOf[String]
    })

    df.withColumn("dd", my_udf($"feature_list")).show()

  }
  test("calculate pop log odds data frame") {
    spark.sharedState.cacheManager.clearCache()
    val data = Seq(
      (
        new SparseVector(3, Array(0, 1, 2), Array(0.2, 0.3, 0.4)),
        new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5)),
        Seq((0, "foo", "foo"), (1, "bar", "foo"), (2, "zoo", "zoo"))
      ),
      (
        new SparseVector(3, Array(0, 2), Array(0.2, 0.4)),
        new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5)),
        Seq((0, "foo", "foo"), (1, "bar", "foo"), (2, "zoo", "zoo"))
      )
    ).toDF()
    val df = data.toDF("v1", "v2", "feature_list")
    // df.withColumn("feature_list", df.col("feature_list").cast("array<struct<_1:int,_2:string,_3:string>>"))

    df.printSchema()
    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new CalculatePopulationContribution().call _

    spark.udf.register("sparse_vector_calculate_population_contribution_log_odds", add_function)

    val out_df = spark.sql(
      "select sparse_vector_calculate_population_contribution_log_odds(v1, v2, feature_list) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(3, Array(0, 1, 2), Array(0.7, 0.7, 0.5))),
        Row(new SparseVector(3, Array(0, 2), Array(0.7, 0.5)))
      )
    )
    assert(2 == out_df.count())
  }
}
