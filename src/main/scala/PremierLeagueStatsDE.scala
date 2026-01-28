import Utils.Constants.SEASON
import Utils.{ConfigurationReader, IOStuff, SparkUtils}
import input.DataLoader
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import transformations.{AllGamesTransformation, ArrayColumnToColumns, AwayHomeGamesTransformation, JoinTables}

import java.util.Properties

object PremierLeagueStatsDE {

  def main(args: Array[String]): Unit = {

    val config: ConfigurationReader = ConfigurationReader(args(0))

    implicit val spark: SparkSession = SparkUtils.createSparkSession()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    implicit val sc: SparkContext = spark.sparkContext
    implicit val mariaDBConnection: (String, Properties) = IOStuff.createMariaDBConnection(config.mariadbHost, config.mariadbPort,
      config.mariadbDatabaseName, config.mariadbUser, config.mariadbPassword)
    sc.setCheckpointDir(config.checkpointDir)

    try {

      val inputMatchesDF: DataFrame = DataLoader.getGameHistory(config, mariaDBConnection)
      val unplayedGamesDF: DataFrame = DataLoader.getUnplayedGames(inputMatchesDF, config, mariaDBConnection)

      val df: DataFrame = inputMatchesDF.unionByName(unplayedGamesDF, allowMissingColumns = true)
      df.cache()
      val initialDFCount: Long = df.count()

      println(f"### initialDFCount = $initialDFCount")

      val allGamesAwayHomeTransformationWithLagDF: DataFrame = AwayHomeGamesTransformation.transform(df)
      allGamesAwayHomeTransformationWithLagDF.checkpoint()
      val allGamesTransformationWithLagDF: DataFrame = AllGamesTransformation.transform(df)
      allGamesTransformationWithLagDF.checkpoint()
      val joinedDF: DataFrame = JoinTables.transform(allGamesAwayHomeTransformationWithLagDF, allGamesTransformationWithLagDF)
      joinedDF.checkpoint()
      val finalDF: DataFrame = ArrayColumnToColumns.transform(joinedDF)

//      finalDF.orderBy(asc("game_datetime")).show(1000, false)

      finalDF.write.mode("overwrite").partitionBy(SEASON).parquet(config.minioDestTableName)

    }
    catch {
      case e: Exception =>
        println("### ERRO!")
        e.printStackTrace()
    }
    finally {
      SparkUtils.deleteCheckpointDirectory(config.checkpointDir)
      SparkUtils.stopSparkSession
    }

  }


}
