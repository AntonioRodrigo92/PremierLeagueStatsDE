package transformations

import Utils.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}
import transformations.CommonFunctions.{getPreviousNOccurrencesAsList, getPreviousNColumnsMean}

object AwayHomeGamesTransformation {

  def transform(df: DataFrame): DataFrame = {
    val newColsDF: DataFrame = addNewCols(df)
    prevGamesInfoDF(newColsDF)
      .select(AWAY_HOME_GAMES_TRANSFORMATION_COLUMNS.head, AWAY_HOME_GAMES_TRANSFORMATION_COLUMNS.tail: _*)
  }
  
  private def addNewCols(df: DataFrame) = {
    df
      .withColumn(AWAY_TEAM_GOALS_AGAINST, col(HOME_TEAM_GOALS))
      .withColumn(HOME_TEAM_GOALS_AGAINST, col(AWAY_TEAM_GOALS))
      .withColumn(AWAY_TEAM_XG_AGAINST, col(HOME_TEAM_XG))
      .withColumn(HOME_TEAM_XG_AGAINST, col(AWAY_TEAM_XG))
      .withColumn(AWAY_TEAM_SHOTS_AGAINST, col(HOME_TEAM_SHOTS))
      .withColumn(HOME_TEAM_SHOTS_AGAINST, col(AWAY_TEAM_SHOTS))
      .withColumn(AWAY_TEAM_SHOTSONTARGET, col(HOME_TEAM_SHOTSONTARGET))
      .withColumn(HOME_TEAM_SHOTSONTARGET, col(AWAY_TEAM_SHOTSONTARGET))
      .withColumn(AWAY_TEAM_POSSESSION_AGAINST, col(HOME_TEAM_POSSESSION))
      .withColumn(HOME_TEAM_POSSESSION_AGAINST, col(AWAY_TEAM_POSSESSION))
      .withColumn(AWAY_TEAM_FOULS_AGAINST, col(HOME_TEAM_FOULS))
      .withColumn(HOME_TEAM_FOULS_AGAINST, col(AWAY_TEAM_FOULS))
      .withColumn(AWAY_TEAM_OFFSIDE_AGAINST, col(HOME_TEAM_OFFSIDE))
      .withColumn(HOME_TEAM_OFFSIDE_AGAINST, col(AWAY_TEAM_OFFSIDE))
      .withColumn(MATCH_RESULT,
        when(col(HOME_TEAM_GOALS) > col(AWAY_TEAM_GOALS), HOME_TEAM_WIN)
          .when(col(AWAY_TEAM_GOALS) > col(HOME_TEAM_GOALS), AWAY_TEAM_WIN)
          .otherwise(DRAW)
      )
  }

  private def prevGamesInfoDF(newColsDF: DataFrame): DataFrame = {
    newColsDF
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_GOALS, HOME_TEAM_HOME_GOALS_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_GOALS, HOME_TEAM_HOME_GOALS_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_GOALS, AWAY_TEAM_AWAY_GOALS_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_GOALS, AWAY_TEAM_AWAY_GOALS_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_XG, HOME_TEAM_HOME_XG_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_XG, HOME_TEAM_HOME_XG_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_XG, AWAY_TEAM_AWAY_XG_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_XG, AWAY_TEAM_AWAY_XG_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_SHOTS, HOME_TEAM_HOME_SHOTS_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_SHOTS, HOME_TEAM_HOME_SHOTS_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_SHOTS, AWAY_TEAM_AWAY_SHOTS_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_SHOTS, AWAY_TEAM_AWAY_SHOTS_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_SHOTSONTARGET, HOME_TEAM_HOME_SHOTSONTARGET_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_SHOTSONTARGET, HOME_TEAM_HOME_SHOTSONTARGET_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_SHOTSONTARGET, AWAY_TEAM_AWAY_SHOTSONTARGET_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_SHOTSONTARGET, AWAY_TEAM_AWAY_SHOTSONTARGET_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_POSSESSION, HOME_TEAM_HOME_POSSESSION_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_POSSESSION, HOME_TEAM_HOME_POSSESSION_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_POSSESSION, AWAY_TEAM_AWAY_POSSESSION_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_POSSESSION, AWAY_TEAM_AWAY_POSSESSION_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_FOULS, HOME_TEAM_HOME_FOULS_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_FOULS, HOME_TEAM_HOME_FOULS_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_FOULS, AWAY_TEAM_AWAY_FOULS_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_FOULS, AWAY_TEAM_AWAY_FOULS_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_OFFSIDE, HOME_TEAM_HOME_OFFSIDE_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_OFFSIDE, HOME_TEAM_HOME_OFFSIDE_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_OFFSIDE, AWAY_TEAM_AWAY_OFFSIDE_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_OFFSIDE, AWAY_TEAM_AWAY_OFFSIDE_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_GOALS_AGAINST, AWAY_TEAM_AWAY_GOALS_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_GOALS_AGAINST, AWAY_TEAM_AWAY_GOALS_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_GOALS_AGAINST, HOME_TEAM_HOME_GOALS_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_GOALS_AGAINST, HOME_TEAM_HOME_GOALS_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_XG_AGAINST, AWAY_TEAM_AWAY_XG_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_XG_AGAINST, AWAY_TEAM_AWAY_XG_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_XG_AGAINST, HOME_TEAM_HOME_XG_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_XG_AGAINST, HOME_TEAM_HOME_XG_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_SHOTS_AGAINST, AWAY_TEAM_AWAY_SHOTS_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_SHOTS_AGAINST, AWAY_TEAM_AWAY_SHOTS_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_SHOTS_AGAINST, HOME_TEAM_HOME_SHOTS_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_SHOTS_AGAINST, HOME_TEAM_HOME_SHOTS_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_POSSESSION_AGAINST, AWAY_TEAM_AWAY_POSSESSION_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_POSSESSION_AGAINST, AWAY_TEAM_AWAY_POSSESSION_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_POSSESSION_AGAINST, HOME_TEAM_HOME_POSSESSION_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_POSSESSION_AGAINST, HOME_TEAM_HOME_POSSESSION_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_FOULS_AGAINST, AWAY_TEAM_AWAY_FOULS_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_FOULS_AGAINST, AWAY_TEAM_AWAY_FOULS_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_FOULS_AGAINST, HOME_TEAM_HOME_FOULS_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_FOULS_AGAINST, HOME_TEAM_HOME_FOULS_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_OFFSIDE_AGAINST, AWAY_TEAM_AWAY_OFFSIDE_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(AWAY_TEAM, AWAY_TEAM_OFFSIDE_AGAINST, AWAY_TEAM_AWAY_OFFSIDE_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_OFFSIDE_AGAINST, HOME_TEAM_HOME_OFFSIDE_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(HOME_TEAM, HOME_TEAM_OFFSIDE_AGAINST, HOME_TEAM_HOME_OFFSIDE_AGAINST_LAST_2_GAMES, 2))

      .transform(getPreviousNOccurrencesAsList(HOME_TEAM, MATCH_RESULT, LST_HOME_TEAM_HOME_MATCH_RESULT_LAST_2_GAMES, 2))

      .transform(getPreviousNOccurrencesAsList(AWAY_TEAM, MATCH_RESULT, LST_AWAY_TEAM_AWAY_MATCH_RESULT_LAST_2_GAMES, 2))
  }

}
