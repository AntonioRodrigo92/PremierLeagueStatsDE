import Utils.{ConfigurationReader, IOStuff, SparkUtils}
import com.typesafe.config.{ConfigFactory, ConfigValue}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import transformations.{AllGamesTransformation, ArrayColumnToColumns, AwayHomeGamesTransformation, JoinTables}

import java.util.Properties
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.sys.exit


object PremierLeagueStatsDE {

  def main(args: Array[String]): Unit = {

    val config: ConfigurationReader = ConfigurationReader(args(0))

    implicit val spark: SparkSession = SparkUtils.createSparkSession()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    implicit val sc: SparkContext = spark.sparkContext
    implicit val mariaDBConnection: (String, Properties) = IOStuff.createMariaDBConnection(config.mariadbHost, config.mariadbPort,
      config.mariadbDatabaseName, config.mariadbUser, config.mariadbPassword)
//    sc.setCheckpointDir(config.checkpointDir)

//    try {

      val inputDFs: Iterable[DataFrame] = config.mariadbSrcTablesName
      .filter(e => config.jsonToElements(e)._1)
      .map(e => spark.read.jdbc(mariaDBConnection._1, config.jsonToElements(e)._2, mariaDBConnection._2))

      val inputMatchesDF: DataFrame = inputDFs.reduce((acc, x) => acc.union(x))
      val predictTableDF: DataFrame = spark.read.jdbc(mariaDBConnection._1, config.mariadbPredictTableName, mariaDBConnection._2)

      val df: DataFrame = inputMatchesDF.union(predictTableDF)
      df.cache()
      val initialDFCount: Long = df.count()

      println(f"### initialDFCount = $initialDFCount")

      val allGamesAwayHomeTransformationWithLagDF: DataFrame = AwayHomeGamesTransformation.transform(df)
//      allGamesAwayHomeTransformationWithLagDF.checkpoint()
      val allGamesTransformationWithLagDF: DataFrame = AllGamesTransformation.transform(df)
//      allGamesTransformationWithLagDF.checkpoint()
      val joinedDF: DataFrame = JoinTables.transform(allGamesAwayHomeTransformationWithLagDF, allGamesTransformationWithLagDF)
//      joinedDF.checkpoint()
      val finalDF: DataFrame = ArrayColumnToColumns.transform(joinedDF)

      finalDF.show(false)

//      finalDF.write.mode("overwrite").partitionBy("season").parquet(config.minioDestTableName)

//    }
//    catch {
//      case e: Exception => {
//        println("### ERRO!")
//        println(e)
//      }
//    }
//    finally {
////      SparkUtils.deleteCheckpointDirectory(config.checkpointDir)
//      SparkUtils.stopSparkSession
//    }

  }


}
