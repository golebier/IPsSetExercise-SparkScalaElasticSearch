package org.scala.gra

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.scala.gra.test.helpers.SparkTestSuit
import org.scalatest.funsuite.AnyFunSuite

/**
 * The ACTest used only to the discover the issue and how to approach the general solution.
 * Not important in the project level, the same U can find in
 *  the @ExtractMutuallyExclusiveRangesBaseACTest and @ExtractMutuallyExclusiveRangesBase
 *  as a later impl. after the discovery process is done.
 *
 *  The redundancy of the code will not be cleaned, as it's the discovery test only.
 */

object IpsConverter {
  private val stringToLongRegex = "\\."
  private val longToStringRegex = """(?<=\G...)"""
  def stringToLong(ip: String): Long = ip.split(stringToLongRegex)
    .map{ x =>
      x.length match {
        case 1 => s"00$x"
        case 2 => s"0$x"
        case _ => x
      }
    }
    .mkString.toLong

  def longToString(ip: Long): String = ip.toString.split(longToStringRegex).map(_.toInt.toString).mkString(".")
}

object IpRangesCleaner {
  // used only for the exploration part!
  def explodeLongSimple(aColName: String, inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    inputDF.flatMap { x => // we could go with case class and .as <- DS!
      val beg = x.getInt(0).toLong
      val end = x.getInt(1).toLong
      (beg to end).toList
    }.toDF(aColName)
  }
}

class BaseLogicDiscoveryACTest extends AnyFunSuite with SparkTestSuit {
  test ("bit simpler approach") {
    val input = Seq(
      (0, 50)
      , (20, 40)
      , (52, 55)
      , (60, 68)
      , (64, 68)
      , (67, 67)
    )
    val expected = Seq(
      (0, 19)
      , (41, 50)
      , (52, 55)
      , (60, 63)
    )

    val aViewName = "except_view"
    val aResultViewName = "result_view"
    val aColName = "ip"
    val aCol = col(aColName)
    import spark.implicits._
    val inputDF = input.toDF("b", "e")
    assert(inputDF.count === 6)
    println(expected.mkString("\n"))
    val exploded = IpRangesCleaner.explodeLongSimple(aColName, inputDF)
    assert(exploded.count === 91)
    testPrintln(exploded)
    val deduplicated = exploded.distinct
    assert(deduplicated.count === 64)
    testPrintln(deduplicated)
    val except = exploded.exceptAll(deduplicated)
    assert(except.count === 27)
    testPrintln(except)
    except.createOrReplaceTempView(aViewName)
    val result = exploded.where(s"$aColName NOT IN (SELECT $aColName FROM $aViewName)") // FIXME make me more optimal
    assert(result.count === 38)
    testPrintln(result)
    result.createTempView(aResultViewName)
    val aFinalResult = spark.sql(s"""
                                    |select
                                    |    min($aColName), max($aColName)
                                    |from (
                                    |    select
                                    |        $aColName, prev_$aColName, new_group,
                                    |        count(case when new_group then 1 end) over (order by $aColName rows between unbounded preceding and current row) group_no   -- tutaj zliczamy ile bylo zmian grupy do biezacego wiersza
                                    |    from (
                                    |        select
                                    |            $aColName, prev_$aColName, prev_$aColName is null or prev_$aColName < $aColName -1 new_group  -- jesli poprzednia wartosc jest mniejsza o wiecej niz 1 to tutaj rozpoczynamy nowa grupe
                                    |        from (
                                    |            select
                                    |                $aColName,
                                    |                LAG($aColName, 1) over (order by $aColName) prev_$aColName
                                    |            from $aResultViewName
                                    |        )
                                    |    )
                                    |)
                                    |group by group_no""".stripMargin)

    println(aFinalResult.collect.mkString("\n"))
  }

  private def testPrintln(df: DataFrame): Unit = println(df.collect.map(_.getLong(0)).sorted.mkString(", "))
  private def testPrintlnString(df: DataFrame): Unit = println(df.collect.map(_.getString(0)).sorted.mkString("\n"))
}
