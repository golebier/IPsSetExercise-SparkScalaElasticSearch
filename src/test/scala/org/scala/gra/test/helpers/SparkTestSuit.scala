package org.scala.gra.test.helpers

import org.apache.spark.sql.SparkSession

trait SparkTestSuit {
  private def master = "local[*]"
  private def theName = this.getClass.getCanonicalName
  implicit val spark: SparkSession = SparkSession.builder.master(master).appName(theName).getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

}
