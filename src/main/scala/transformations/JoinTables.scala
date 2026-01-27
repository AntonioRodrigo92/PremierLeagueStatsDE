package transformations

import Utils.Constants.{AWAY_TEAM, AWAY_TEAM_LST_AWAY_MATCH_RESULT_LAST_2_GAMES, JOINED_TABLE_COLUMNS, GAME_DATETIME, HOME_TEAM, HOME_TEAM_LST_HOME_MATCH_RESULT_LAST_2_GAMES, LST_AWAY_TEAM_AWAY_MATCH_RESULT_LAST_2_GAMES, LST_HOME_TEAM_HOME_MATCH_RESULT_LAST_2_GAMES, MATCH_RESULT, PLAYED_AT, TEAM}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object JoinTables {

  def transform(baseDF: DataFrame, normalizedDF: DataFrame): DataFrame = {
    val homeDF: DataFrame = normalizedDF.filter(col(PLAYED_AT) === "HOME")
    val awayDF: DataFrame = normalizedDF.filter(col(PLAYED_AT) === "AWAY")

    val homeRenamedDF: DataFrame = homeDF.select(
      homeDF.columns.map(c => col(c).as(f"${HOME_TEAM}_$c")): _*
    )

    val awayRenamedDF: DataFrame = awayDF.select(
      awayDF.columns.map(c => col(c).as(f"${AWAY_TEAM}_$c")): _*
    )

    baseDF
      .join(
        homeRenamedDF,
        baseDF(GAME_DATETIME) === homeRenamedDF(f"${HOME_TEAM}_${GAME_DATETIME}")
        && baseDF(HOME_TEAM) === homeRenamedDF(f"${HOME_TEAM}_$TEAM"),
        "left"
    ).drop(homeRenamedDF(f"${HOME_TEAM}_$GAME_DATETIME"), homeRenamedDF(f"${HOME_TEAM}_$TEAM"),
      homeRenamedDF(f"${HOME_TEAM}_$PLAYED_AT"), homeRenamedDF(f"${HOME_TEAM}_$MATCH_RESULT"))
      .join(
        awayRenamedDF,
        baseDF(GAME_DATETIME) === awayRenamedDF(f"${AWAY_TEAM}_${GAME_DATETIME}")
          && baseDF(AWAY_TEAM) === awayRenamedDF(f"${AWAY_TEAM}_$TEAM"),
        "left"
    ).drop(awayRenamedDF(f"${AWAY_TEAM}_$GAME_DATETIME"), awayRenamedDF(f"${AWAY_TEAM}_$TEAM"),
      awayRenamedDF(f"${AWAY_TEAM}_$PLAYED_AT"), awayRenamedDF(f"${AWAY_TEAM}_$MATCH_RESULT"))
      .withColumnRenamed(LST_HOME_TEAM_HOME_MATCH_RESULT_LAST_2_GAMES, HOME_TEAM_LST_HOME_MATCH_RESULT_LAST_2_GAMES)
      .withColumnRenamed(LST_AWAY_TEAM_AWAY_MATCH_RESULT_LAST_2_GAMES, AWAY_TEAM_LST_AWAY_MATCH_RESULT_LAST_2_GAMES)
      .select(JOINED_TABLE_COLUMNS.head, JOINED_TABLE_COLUMNS.tail: _*)

  }

}
