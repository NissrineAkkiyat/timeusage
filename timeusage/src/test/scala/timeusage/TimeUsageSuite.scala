package timeusage

import munit.FunSuite
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset}



class TimeUsageSuite extends FunSuite {

  val timeUsage: TimeUsage.type = TimeUsage

  val (columns, initDf) = timeUsage.read("src/main/resources/timeusage/atussum.csv")
  val (primaryNeedsColumns, workColumns, otherColumns) = timeUsage.classifiedColumns(columns)
  val summaryDf: DataFrame = timeUsage.timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
  val finalDf: DataFrame = timeUsage.timeUsageGrouped(summaryDf)

  val sqlDf: DataFrame = timeUsage.timeUsageGroupedSql(summaryDf)
  val summaryDs: Dataset[TimeUsageRow] = timeUsage.timeUsageSummaryTyped(summaryDf)
  val finalDs: Dataset[TimeUsageRow] = timeUsage.timeUsageGroupedTyped(summaryDs)

//  test("timeUsage") {
//    assertEquals(timeUsage.spark.sparkContext.appName, "Time Usage")
//    assert(!timeUsage.spark.sparkContext.isStopped)
//  }
  test("timeUsage") {
    assertEquals(timeUsage.spark.sparkContext.appName, "Time Usage", "Nom de l'application incorrect")
    assert(!timeUsage.spark.sparkContext.isStopped, "Le contexte Spark est arrêté")
  }


  test("row") {
    val testRow = timeUsage.row(List("fieldA", "0.3", "1"))

    assertEquals(testRow(0).getClass.getName, "java.lang.String")
    assertEquals(testRow(1).getClass.getName, "java.lang.Double")
    assertEquals(testRow(2).getClass.getName, "java.lang.Double")
  }

  test("read") {
    assertEquals(columns.length, 455)
    initDf.show()
  }

  test("classifiedColumns") {
    val pnC = primaryNeedsColumns.map(_.toString)
    val wC = workColumns.map(_.toString)
    val oC = otherColumns.map(_.toString)

    assert(pnC.contains("t010199"))
    assert(pnC.contains("t030501"))
    assert(pnC.contains("t110101"))
    assert(pnC.contains("t180382"))
    assert(wC.contains("t050103"))
    assert(wC.contains("t180589"))
    assert(oC.contains("t020101"))
    assert(oC.contains("t180699"))
  }

  test("timeUsageSummary") {
    val summaryDf = timeUsage.timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    assertEquals(summaryDf.columns.length, 6, "Nombre de colonnes incorrect dans le DataFrame de résumé")
    assertEquals(summaryDf.count.toInt, 114997, "Nombre de lignes incorrect dans le DataFrame de résumé")
  }


  test("timeUsageGrouped") {
    assertEquals(finalDf.count.toInt, 2 * 2 * 3)
    assertEquals(finalDf.head.getDouble(3), 12.4)
    finalDf.show()
  }

  test("timeUsageGroupedSql") {
    assertEquals(sqlDf.count.toInt, 2 * 2 * 3)
    assertEquals(sqlDf.head.getDouble(3), 12.4)
    sqlDf.show()
  }

  test("timeUsageSummaryTyped") {
    assertEquals(summaryDs.head.getClass.getName, "timeusage.TimeUsageRow")
    assertEquals(summaryDs.head.other, 8.75)
    assertEquals(summaryDs.count.toInt, 114997)
    summaryDs.show()
  }

  test("timeUsageGroupedTyped") {
    assertEquals(finalDs.count.toInt, 2 * 2 * 3)
    assertEquals(finalDs.head.primaryNeeds, 12.4)
    assertEquals(finalDs.head.work, 0.5)
  }
}
