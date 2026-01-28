package transformations

import Utils.Constants.{GAME_DATETIME, SEASON}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{avg, col, collect_list}

object CommonFunctions {

  def getPreviousNColumnsMean(partitionColumn: String, baseColumnName: String,
                              outputColumnName: String, lastNEvents: Int)
                             (df: DataFrame): DataFrame = {
    val window: WindowSpec = Window
      .partitionBy(SEASON, partitionColumn)
      .orderBy(col(GAME_DATETIME).asc)

    df.withColumn(outputColumnName, avg(baseColumnName).over(window.rowsBetween(-lastNEvents, -1)))
  }

  def getPreviousNOccurrencesAsList(partitionColumn: String, baseColumnName: String,
                                    outputColumnName: String, lastNEvents: Int)
                                   (df: DataFrame): DataFrame = {
    val window: WindowSpec = Window
      .partitionBy(SEASON, partitionColumn)
      .orderBy(col(GAME_DATETIME).asc)

    df.withColumn(outputColumnName, collect_list(baseColumnName).over(window.rowsBetween(-lastNEvents, -1)))
  }

  def listToColumns(baseColumnName: String, lastNEvents: Int, prefix: String)(df: DataFrame): DataFrame = {
    val columnExpres = (0 until lastNEvents)
      .map(i => {
        col(baseColumnName)(i).as(f"${prefix}_${nthFromLast(lastNEvents - i - 1)}last_match_result")
      }
    )
    df.select(col("*") +: columnExpres: _*)
  }

  private def nthFromLast(i: Int): String = {
    i match {
      case 0 => ""
      case 1 => "2nd_from_"
      case 2 => "3rd_from_"
      case _ => f"${i + 1}th_from_"
    }
  }

}
