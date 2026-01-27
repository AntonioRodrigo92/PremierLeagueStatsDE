package transformations

import Utils.Constants.{AWAY_TEAM, AWAY_TEAM_LST_AWAY_MATCH_RESULT_LAST_2_GAMES, AWAY_TEAM_LST_MATCH_RESULT_LAST_5_GAMES, FINAL_TABLE_COLUMNS, HOME_TEAM, HOME_TEAM_LST_HOME_MATCH_RESULT_LAST_2_GAMES, HOME_TEAM_LST_MATCH_RESULT_LAST_5_GAMES}
import org.apache.spark.sql.DataFrame
import transformations.CommonFunctions.listToColumns

object ArrayColumnToColumns {

  def transform(dataframe: DataFrame): DataFrame = {
    dataframe
      .transform(listToColumns(HOME_TEAM_LST_MATCH_RESULT_LAST_5_GAMES, 5, HOME_TEAM))
      .transform(listToColumns(HOME_TEAM_LST_HOME_MATCH_RESULT_LAST_2_GAMES, 2, f"${HOME_TEAM}_home"))
      .transform(listToColumns(AWAY_TEAM_LST_MATCH_RESULT_LAST_5_GAMES, 5, AWAY_TEAM))
      .transform(listToColumns(AWAY_TEAM_LST_AWAY_MATCH_RESULT_LAST_2_GAMES, 2, f"${AWAY_TEAM}_away"))
      .select(FINAL_TABLE_COLUMNS.head, FINAL_TABLE_COLUMNS.tail: _*)
  }

}
