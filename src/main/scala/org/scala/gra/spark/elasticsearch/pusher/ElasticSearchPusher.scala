package org.scala.gra.spark.elasticsearch.pusher

import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._

/**
 * Elastic search writer.
 */
case class ElasticSearchPusher() {
  /**
   * Push @DataFrame to ElasticSearch
   * @param df: @DataFrame pushed to ElasticSearch
   */
  def push(df: DataFrame): Unit = df.saveToEs("mutually/exclusive")
}

case class ElasticSearchPusherArgsHolder(nodes: String, port: String)

/**
 * simplification: we should use args with proper parameters!
 */
object ElasticSearchPusher {
  def buildProperties(args: Array[String]): ElasticSearchPusherArgsHolder = {
    ElasticSearchPusherArgsHolder("es01,es02,es03", "9200") // TODO it should be taken from properties/settings.
  }
}
