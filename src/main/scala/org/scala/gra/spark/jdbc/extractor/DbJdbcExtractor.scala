package org.scala.gra.spark.jdbc.extractor

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The DB connection and data operation object.
 * @param connectionProp @DbJdbcExtractorArgsHolder the connection properties
 * @param spark @SparkSession used for the jdbc connection
 */
case class DbJdbcExtractor(connectionProp: DbJdbcExtractorArgsHolder)(implicit spark: SparkSession) {
  private val fullLoadQuery = s"""SELECT * FROM ${connectionProp.db}.${connectionProp.table}"""

  /**
   * Pull all data from the JDBC connection to the database.
   * @return @DataFrame with the data pulled from the DB.
   */
  def fullLoad: DataFrame = spark.read
    .format("jdbc")
    .option("url", connectionProp.url)
    .option("driver", connectionProp.driver)
    .option("query", fullLoadQuery)
    .option("user", connectionProp.user)
    .option("password", connectionProp.passwd)
    .option("encoding", "UTF-8")
    .option("charset", "UTF-8")
    .option("characterEncoding", "UTF-8")
    .load
    .toDF
}

case class DbJdbcExtractorArgsHolder(url: String, driver: String, user: String, passwd: String, db: String, table: String)

/**
 * simplification: we should use args with proper parameters!
 */
object DbJdbcExtractor {
  def buildProperties(args: Array[String]): DbJdbcExtractorArgsHolder = {
    DbJdbcExtractorArgsHolder("jdbc:mysql://mysql:3306/", "com.mysql.jdbc.Driver", "gra", "gra", "gra", "ranges") // TODO it should be taken from properties/settings.
  }
}
