package org.scala.gra.spark.job

import org.apache.spark.sql.{DataFrame, Row}
import org.scala.gra.spark.elasticsearch.pusher.ElasticSearchPusher
import org.scala.gra.spark.jdbc.extractor.DbJdbcExtractor
import org.scala.gra.test.helpers.SparkTestSuit
import org.scalatest.funsuite.AnyFunSuite

class ExtractMutuallyExclusiveRangesBaseACTest extends AnyFunSuite with SparkTestSuit {
  val theColumnNames = List("s", "e")
  object TestDbJdbcExtractor extends DbJdbcExtractor(DbJdbcExtractor.buildProperties(Array.empty[String])) {
    import spark.implicits._
    override def fullLoad: DataFrame = Seq(
      (0, 50)
      , (20, 40)
      , (52, 55)
      , (60, 68)
      , (64, 68)
      , (67, 67)
    ).toDF(theColumnNames: _*)
  }
  object TestElasticSearchPusher extends ElasticSearchPusher {
    // setup ES
    var pushed: DataFrame = _
    override def push(df: DataFrame): Unit = pushed = df
  }

  test ("e2e the spark job") {
    // init
    import spark.implicits._
    val expected = Seq(
      (0, 19)
      , (41, 50)
      , (52, 55)
      , (60, 63)
    ).toDF(theColumnNames: _*)
    implicit val db: DbJdbcExtractor = TestDbJdbcExtractor // DbJdbcExtractor(DbJdbcExtractor.buildProperties(Array.empty[String]))
    implicit val es: ElasticSearchPusher = TestElasticSearchPusher

    val tested = ExtractMutuallyExclusiveRangesBase()
    // run
    tested.main(Array.empty[String])
    // validate
    assert(TestElasticSearchPusher.pushed.exceptAll(expected).rdd.isEmpty)
  }
}
