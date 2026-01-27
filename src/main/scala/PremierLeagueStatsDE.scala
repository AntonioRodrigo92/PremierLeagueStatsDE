import Utils.{ConfigurationReader, IOStuff, SparkUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import transformations.{AllGamesTransformation, ArrayColumnToColumns, AwayHomeGamesTransformation, JoinTables}


object PremierLeagueStatsDE {

  def main(args: Array[String]): Unit = {

    val config: ConfigurationReader = ConfigurationReader(args(0))

    implicit val spark: SparkSession = SparkUtils.createSparkSession()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    implicit val sc: SparkContext = spark.sparkContext
    implicit val mariaDBConnection = IOStuff.createMariaDBConnection(config.mariadbHost, config.mariadbPort,
      config.mariadbDatabaseName, config.mariadbUser, config.mariadbPassword)
    sc.setCheckpointDir(config.checkpointDir)

    try {
      val df: DataFrame = spark.read.jdbc(mariaDBConnection._1, config.mariadbSrcTableName, mariaDBConnection._2)
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

//      finalDF.show(false)

      finalDF.write.mode("overwrite").partitionBy("season").parquet(config.minioDestTableName)

    }
    catch {
      case e: Exception => {
        println("### ERRO!")
        println(e)
      }
    }
    finally {
      SparkUtils.deleteCheckpointDirectory(config.checkpointDir)
      SparkUtils.stopSparkSession
    }

  }


}
