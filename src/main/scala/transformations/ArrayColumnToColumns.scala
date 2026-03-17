package transformations

import Utils.Constants.{AWAY_TEAM, AWAY_TEAM_LST_AWAY_MATCH_RESULT_LAST_2_GAMES, AWAY_TEAM_LST_MATCH_RESULT_LAST_5_GAMES, FINAL_TABLE_COLUMNS, GAME_DATE, GAME_DATETIME, GAME_TIME, HOME_TEAM, HOME_TEAM_LST_HOME_MATCH_RESULT_LAST_2_GAMES, HOME_TEAM_LST_MATCH_RESULT_LAST_5_GAMES}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.types.{DateType, StringType, TimestampType}
import transformations.CommonFunctions.listToColumns

object ArrayColumnToColumns {

  def transform(dataframe: DataFrame): DataFrame = {
    dataframe
      .transform(listToColumns(HOME_TEAM_LST_MATCH_RESULT_LAST_5_GAMES, 5, HOME_TEAM))
      .transform(listToColumns(HOME_TEAM_LST_HOME_MATCH_RESULT_LAST_2_GAMES, 2, f"${HOME_TEAM}_home"))
      .transform(listToColumns(AWAY_TEAM_LST_MATCH_RESULT_LAST_5_GAMES, 5, AWAY_TEAM))
      .transform(listToColumns(AWAY_TEAM_LST_AWAY_MATCH_RESULT_LAST_2_GAMES, 2, f"${AWAY_TEAM}_away"))
      .select(FINAL_TABLE_COLUMNS.head, FINAL_TABLE_COLUMNS.tail: _*)
      .withColumn(GAME_DATETIME, col(GAME_DATETIME).cast(TimestampType))
      .withColumn(GAME_DATE, col(GAME_DATE).cast(DateType))
      .withColumn(GAME_TIME, substring(col(GAME_TIME).cast(StringType), -8, 8))
  }

}
