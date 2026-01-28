package input

import Utils.ConfigurationReader
import Utils.Constants.GAME_DATETIME
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.util.Properties
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object DataLoader {

  def getGameHistory(config: ConfigurationReader, mariaDBConnection: (String, Properties))
                    (implicit spark: SparkSession): DataFrame = {

    val inputDFs: Iterable[DataFrame] = config.mariadbSrcTablesName
      .filter(e => config.jsonToElements(e)._1)
      .map(e => spark.read.jdbc(mariaDBConnection._1, config.jsonToElements(e)._2, mariaDBConnection._2))

    inputDFs.reduce((acc, x) => acc.union(x))
  }

  def getUnplayedGames(gameHistoryDF: DataFrame, config: ConfigurationReader, mariaDBConnection: (String, Properties))
                       (implicit spark: SparkSession): DataFrame = {

    val maxDate: Timestamp = gameHistoryDF.agg(max(GAME_DATETIME)).head().getTimestamp(0)
    val predictTableDF: DataFrame = spark.read.jdbc(mariaDBConnection._1, config.mariadbUnplayedGamesTableName, mariaDBConnection._2)
    predictTableDF.filter(col(GAME_DATETIME) > maxDate)

  }

}
