package transformations

import Utils.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}
import transformations.CommonFunctions.{getPreviousNColumnsMean, getPreviousNOccurrencesAsList}


object AllGamesTransformation {

  def transform(dataframe: DataFrame) = {
    val normalizedDF: DataFrame = normalizePerTeamDF(dataframe)
    prevGamesInfoDF(normalizedDF)
      .select(ALL_GAMES_TRANSFORMATION_COLUMNS.head, ALL_GAMES_TRANSFORMATION_COLUMNS.tail: _*)
  }

  private def prevGamesInfoDF(normalizePerTeamDF: DataFrame): DataFrame = {
    normalizePerTeamDF
      .transform(getPreviousNColumnsMean(TEAM, GOALS_SCORED, GOALS_SCORED_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, GOALS_SCORED, AVG_GOALS_SCORED_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, GOALS_SCORED, AVG_GOALS_SCORED_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, GOALS_SCORED_AGAINST, GOALS_SCORED_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, GOALS_SCORED_AGAINST, AVG_GOALS_SCORED_AGAINST_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, GOALS_SCORED_AGAINST, AVG_GOALS_SCORED_AGAINST_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, XG, XG_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, XG, AVG_XG_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, XG, AVG_XG_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, XG_AGAINST, XG_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, XG_AGAINST, AVG_XG_AGAINST_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, XG_AGAINST, AVG_XG_AGAINST_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, XG_AGAINST, XG_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, XG_AGAINST, AVG_XG_AGAINST_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, XG_AGAINST, AVG_XG_AGAINST_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, SHOTS, SHOTS_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, SHOTS, AVG_SHOTS_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, SHOTS, AVG_SHOTS_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, SHOTS_AGAINST, SHOTS_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, SHOTS_AGAINST, AVG_SHOTS_AGAINST_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, SHOTS_AGAINST, AVG_SHOTS_AGAINST_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, SHOTSONTARGET, SHOTSONTARGET_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, SHOTSONTARGET, AVG_SHOTSONTARGET_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, SHOTSONTARGET, AVG_SHOTSONTARGET_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, SHOTSONTARGET_AGAINST, SHOTSONTARGET_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, SHOTSONTARGET_AGAINST, AVG_SHOTSONTARGET_AGAINST_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, SHOTSONTARGET_AGAINST, AVG_SHOTSONTARGET_AGAINST_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, POSSESSION, POSSESSION_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, POSSESSION, AVG_POSSESSION_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, POSSESSION, AVG_POSSESSION_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, POSSESSION_AGAINST, POSSESSION_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, POSSESSION_AGAINST, AVG_POSSESSION_AGAINST_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, POSSESSION_AGAINST, AVG_POSSESSION_AGAINST_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, FOULS, FOULS_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, FOULS, AVG_FOULS_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, FOULS, AVG_FOULS_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, FOULS, FOULS_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, FOULS, AVG_FOULS_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, FOULS, AVG_FOULS_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, FOULS_AGAINST, FOULS_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, FOULS_AGAINST, AVG_FOULS_AGAINST_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, FOULS_AGAINST, AVG_FOULS_AGAINST_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, OFFSIDE, OFFSIDE_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, OFFSIDE, AVG_OFFSIDE_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, OFFSIDE, AVG_OFFSIDE_LAST_5_GAMES, 5))

      .transform(getPreviousNColumnsMean(TEAM, OFFSIDE_AGAINST, OFFSIDE_AGAINST_LAST_GAME, 1))
      .transform(getPreviousNColumnsMean(TEAM, OFFSIDE_AGAINST, AVG_OFFSIDE_AGAINST_LAST_3_GAMES, 3))
      .transform(getPreviousNColumnsMean(TEAM, OFFSIDE_AGAINST, AVG_OFFSIDE_AGAINST_LAST_5_GAMES, 5))

      .transform(getPreviousNOccurrencesAsList(TEAM, MATCH_RESULT, LST_MATCH_RESULT_LAST_5_GAMES, 5))

  }

  private def normalizePerTeamDF(df: DataFrame) = {
    df
      // home team
      .select(
        col(GAME_DATETIME),
        col(GAME_DATE),
        col(GAME_TIME),
        col(HOME_TEAM).as(TEAM),
        col(HOME_TEAM_GOALS).as(GOALS_SCORED),
        col(AWAY_TEAM_GOALS).as(GOALS_SCORED_AGAINST),
        col(HOME_TEAM_XG).as(XG),
        col(AWAY_TEAM_XG).as(XG_AGAINST),
        col(HOME_TEAM_SHOTS).as(SHOTS),
        col(AWAY_TEAM_SHOTS).as(SHOTS_AGAINST),
        col(HOME_TEAM_SHOTSONTARGET).as(SHOTSONTARGET),
        col(AWAY_TEAM_SHOTSONTARGET).as(SHOTSONTARGET_AGAINST),
        col(HOME_TEAM_POSSESSION).as(POSSESSION),
        col(AWAY_TEAM_POSSESSION).as(POSSESSION_AGAINST),
        col(HOME_TEAM_FOULS).as(FOULS),
        col(AWAY_TEAM_FOULS).as(FOULS_AGAINST),
        col(HOME_TEAM_OFFSIDE).as(OFFSIDE),
        col(AWAY_TEAM_OFFSIDE).as(OFFSIDE_AGAINST),
        col(AWAY_TEAM).as(PLAYED_AGAINST),
        lit("HOME").as(PLAYED_AT)
      )
      //  away team
      .unionAll(
        df.select(
          col(GAME_DATETIME),
          col(GAME_DATE),
          col(GAME_TIME),
          col(AWAY_TEAM).as(TEAM),
          col(AWAY_TEAM_GOALS).as(GOALS_SCORED),
          col(HOME_TEAM_GOALS).as(GOALS_SCORED_AGAINST),
          col(AWAY_TEAM_XG).as(XG),
          col(HOME_TEAM_XG).as(XG_AGAINST),
          col(AWAY_TEAM_SHOTS).as(SHOTS),
          col(HOME_TEAM_SHOTS).as(SHOTS_AGAINST),
          col(AWAY_TEAM_SHOTSONTARGET).as(SHOTSONTARGET),
          col(HOME_TEAM_SHOTSONTARGET).as(SHOTSONTARGET_AGAINST),
          col(AWAY_TEAM_POSSESSION).as(POSSESSION),
          col(HOME_TEAM_POSSESSION).as(POSSESSION_AGAINST),
          col(AWAY_TEAM_FOULS).as(FOULS),
          col(HOME_TEAM_FOULS).as(FOULS_AGAINST),
          col(AWAY_TEAM_OFFSIDE).as(OFFSIDE),
          col(HOME_TEAM_OFFSIDE).as(OFFSIDE_AGAINST),
          col(HOME_TEAM).as(PLAYED_AGAINST),
          lit("AWAY").as(PLAYED_AT)
        )
      )
      //  match result
      .withColumn(MATCH_RESULT,
        when(col(GOALS_SCORED) > col(GOALS_SCORED_AGAINST), WIN)
          .when(col(GOALS_SCORED_AGAINST) > col(GOALS_SCORED), LOSS)
          .otherwise(DRAW)
      )
  }

}
