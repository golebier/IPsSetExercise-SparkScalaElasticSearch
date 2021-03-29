package org.scala.gra.spark.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scala.gra.spark.elasticsearch.pusher.ElasticSearchPusher
import org.scala.gra.spark.jdbc.extractor.DbJdbcExtractor

/**
 * The core calculation implementation.
 * @DbJdbcExtractor pulls input data from DB
 * @SparkSession calculates the mutually exclusive ranges
 * @ElasticSearchPusher pushes the results with ranges to ElasticSearch.
 *
 * @param spark @SparkSesssion core calculation engine
 * @param db @DbJdbcExtractor abstraction on the JDBC connection to the DB
 * @param es @ElasticSearchPusher abstraction on the ElasticSearch connection
 */
case class ExtractMutuallyExclusiveRangesBase(implicit spark: SparkSession, db: DbJdbcExtractor, es: ElasticSearchPusher) {
  private val aViewName = "except_view"
  private val aResultViewName = "result_view"
  private val aColName = "ip"

  /**
   * Main of the spark Job, takes the input data from @DbJdbcExtractor, computes mutually exclusive ranges of IPs
   * the result pushes to @ElasticSearchPusher
   *
   * @param args @Array[String] not used on a present version.
   */
  def main(args: Array[String]): Unit = {
    val inputDF = db.fullLoad
    val exploded = explodeLongSimple(inputDF)
    val deduplicated = exploded.distinct
    val except = exploded.exceptAll(deduplicated)
    except.createOrReplaceTempView(aViewName)
    val result = exploded.where(s"$aColName NOT IN (SELECT $aColName FROM $aViewName)")
    result.createTempView(aResultViewName)
    val aFinalResult = spark.sql(
      s"""select
      |    min($aColName), max($aColName)
      |from (
      |    select
      |        $aColName, prev_$aColName, new_group,
      |        count(case when new_group then 1 end) over (order by $aColName rows between unbounded preceding and current row) group_no
      |    from (
      |        select
      |            $aColName, prev_$aColName, prev_$aColName is null or prev_$aColName < $aColName -1 new_group
      |        from (
      |            select $aColName, LAG($aColName, 1) over (order by $aColName) prev_$aColName
      |            from $aResultViewName
      |        )
      |    )
      |)
      |group by group_no""".stripMargin
    )
    es.push(aFinalResult)
  }

  private def explodeLongSimple(inputDF: DataFrame): DataFrame = {
    import spark.implicits._
    inputDF.flatMap { x => // we could go with case class and .as <- DS!
      val beg = x.getInt(0).toLong
      val end = x.getInt(1).toLong
      (beg to end).toList
    }.toDF(aColName)
  }
}

/**
 * Spark Job object.
 */
object ExtractMutuallyExclusiveRanges {
  private def theName = this.getClass.getCanonicalName

  def main(args: Array[String]): Unit = {
    val esConnectionData = ElasticSearchPusher.buildProperties(args)
    implicit val spark: SparkSession = SparkSession.builder.appName(theName)
      .config("spark.es.nodes", esConnectionData.nodes)
      .config("spark.es.port", esConnectionData.port)
      .config("spark.es.nodes.wan.only","true") // Needed for ES on AWS
      .getOrCreate
    implicit val db: DbJdbcExtractor = DbJdbcExtractor(DbJdbcExtractor.buildProperties(args))
    implicit val es: ElasticSearchPusher = ElasticSearchPusher()

    ExtractMutuallyExclusiveRangesBase().main(args)
  }
}
